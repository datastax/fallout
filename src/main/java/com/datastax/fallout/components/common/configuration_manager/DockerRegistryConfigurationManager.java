/*
 * Copyright 2021 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.fallout.components.common.configuration_manager;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.auto.service.AutoService;

import com.datastax.fallout.components.common.provider.DockerProvider;
import com.datastax.fallout.components.kubernetes.KubeControlProvider;
import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;

import static com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec.buildNameSpaceSpec;
import static com.datastax.fallout.service.core.User.DockerRegistryCredential;

@AutoService(ConfigurationManager.class)
public class DockerRegistryConfigurationManager extends ConfigurationManager
{
    private static final String PREFIX = "fallout.configuration.management.docker_registry.";

    private static final PropertySpec<String> dockerRegistrySpec = PropertySpecBuilder.createStr(PREFIX)
        .name("registry")
        .description("Name of the docker registry to pull images from. Must be present in your user profile.")
        .required()
        .build();

    private static final PropertySpec<String> namespaceSpec = buildNameSpaceSpec(PREFIX);

    private static final PropertySpec<String> secretNameSpec = PropertySpecBuilder.createStr(PREFIX)
        .name("secret_name")
        .description("Required for Kubernetes cluster, the name of the secret to create.")
        .build();

    @Override
    public String prefix()
    {
        return PREFIX;
    }

    @Override
    public String name()
    {
        return "docker_registry";
    }

    @Override
    public String description()
    {
        return "Configures the credentials required for pulling images from a private docker registry. For kubernetes clusters this will create a secret. On VMs this will login via docker cli.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.empty();
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(dockerRegistrySpec, namespaceSpec, secretNameSpec);
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        // This will throw if the specified registry credential does not exist
        getRegistryCredential();
        if (getNodeGroup().willHaveProvider(KubeControlProvider.class))
        {
            if (secretNameSpec.optionalValue(properties).isEmpty())
                throw new PropertySpec.ValidationException(
                    String.format("Missing required property: %s", secretNameSpec.shortName()));
        }
        else if (secretNameSpec.optionalValue(properties).isPresent())
        {
            throw new PropertySpec.ValidationException(
                String.format("Cannot specify property (%s) on VM based node group", secretNameSpec.shortName()));
        }
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        if (!getNodeGroup().willHaveProvider(KubeControlProvider.class) &&
            !getNodeGroup().willHaveProvider(DockerProvider.class))
        {
            throw new PropertySpec.ValidationException(String.format("Missing required Docker provider"));
        }
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        Optional<String> osda = secretNameSpec.optionalValue(nodeGroupProperties);
        Set<Class<? extends Provider>> ret = osda.isEmpty() ?
            Set.of(DockerProvider.class) : Set.of();
        return ret;
    }

    private <T> T handleForKubernetesOrVm(Function<KubeControlProvider.NamespacedKubeCtl, T> kubernetesCase,
        Supplier<T> vmCase)
    {
        return getNodeGroup().findFirstProvider(KubeControlProvider.class)
            .map(kubeControlProvider -> kubeControlProvider.inNamespace(namespaceSpec.optionalValue(getNodeGroup()),
                kubernetesCase))
            .orElseGet(vmCase);
    }

    private DockerRegistryCredential getRegistryCredential()
    {
        return getNodeGroup().getCredentials().getUser()
            .getDockerRegistryCredential(dockerRegistrySpec.value(getNodeGroup()))
            .orElseThrow(() -> new InvalidConfigurationException(String.format(
                "No Docker registry credential found matching specified %s", dockerRegistrySpec.shortName())));
    }

    @Override
    protected boolean configureImpl(NodeGroup nodeGroup)
    {
        return handleForKubernetesOrVm(this::createKubernetesSecret, this::loginViaDocker);
    }

    @Override
    protected boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return handleForKubernetesOrVm(this::deleteKubernetesSecret, this::logoutViaDocker);
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return handleForKubernetesOrVm(this::checkKubernetesSecretExists, this::checkLoggedIntoRegistry);
    }

    private NodeGroup.State checkKubernetesSecretExists(KubeControlProvider.NamespacedKubeCtl kubeCtl)
    {
        FullyBufferedNodeResponse getSecretNames =
            kubeCtl.execute("get secrets -o=jsonpath={$.items..metadata.name}").buffered();
        if (!getSecretNames.waitForSuccess())
        {
            getNodeGroup().logger().error("Failed to lookup secrets!");
            return NodeGroup.State.FAILED;
        }
        return getSecretNames.getStdout().contains(secretNameSpec.value(getNodeGroup())) ?
            NodeGroup.State.STARTED_SERVICES_RUNNING :
            NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    private boolean createKubernetesSecret(KubeControlProvider.NamespacedKubeCtl kubeCtl)
    {
        return kubeCtl.createDockerSecret(secretNameSpec.value(getNodeGroup()), getRegistryCredential());
    }

    private boolean deleteKubernetesSecret(KubeControlProvider.NamespacedKubeCtl kubeCtl)
    {
        return kubeCtl.deleteSecret(secretNameSpec.value(getNodeGroup()));
    }

    private NodeGroup.State checkLoggedIntoRegistry()
    {
        return getNodeGroup().waitForSuccess(
            String.format("cat ~/.docker/config.json |  grep '%s'", getRegistryCredential().dockerRegistry)) ?
                NodeGroup.State.STARTED_SERVICES_RUNNING :
                NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    private boolean loginViaDocker()
    {
        DockerRegistryCredential cred = getRegistryCredential();
        return getNodeGroup().waitForSuccess(String.format("docker login -u '%s' -p '%s' https://%s",
            cred.username, cred.password, cred.dockerRegistry));
    }

    private boolean logoutViaDocker()
    {
        return getNodeGroup().waitForSuccess(String.format("docker logout %s", getRegistryCredential().dockerRegistry));
    }
}
