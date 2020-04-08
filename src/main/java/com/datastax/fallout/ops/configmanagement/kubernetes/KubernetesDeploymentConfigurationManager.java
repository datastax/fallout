/*
 * Copyright 2020 DataStax, Inc.
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
package com.datastax.fallout.ops.configmanagement.kubernetes;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.io.Resources;

import com.datastax.fallout.harness.specs.KubernetesManifestSpec;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.providers.KubeControlProvider;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.harness.TestDefinition.renderDefinitionWithScopes;
import static com.datastax.fallout.ops.configmanagement.kubernetes.KubernetesManifestConfigurationManager.applyAndWaitForManifest;

/** Configuration Manager for Kubernetes Deployment objects based on templates kept as Fallout resource.
 *  Templating allows different replica and images, as well as handling naming and labels in a consistent manner.
 */
public abstract class KubernetesDeploymentConfigurationManager extends ConfigurationManager
{
    // Kubernetes labels must match. See https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
    private static final Pattern DNS1123 =
        Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");

    private final String prefix;
    private final String name;
    private final String description;
    private final String template;

    private final PropertySpec<String> namespaceSpec;
    private final PropertySpec<Integer> replicaSpec;
    private final PropertySpec<String> imageSpec;

    public KubernetesDeploymentConfigurationManager(String prefix, String name, String description,
        String imageDefault, String template)
    {
        Preconditions.checkArgument(DNS1123.matcher(name).matches(),
            "Implementations of KubernetesDeploymentConfigurationManager must use a name which is DNS1123 compliant");

        this.prefix = prefix;
        this.name = name;
        this.description = description;
        this.template = template;

        namespaceSpec = KubernetesManifestSpec.buildNameSpaceSpec(prefix);
        replicaSpec = PropertySpecBuilder.createInt(prefix)
            .name("replicas")
            .description("Total number of pod replicas in the deployment.")
            .required()
            .build();
        imageSpec = PropertySpecBuilder.createStr(prefix)
            .name("image")
            .description("Image to deploy.")
            .defaultOf(imageDefault)
            .build();
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String description()
    {
        return description;
    }

    protected String getPodLabelSelector()
    {
        return String.format("app=%s", name);
    }

    protected Optional<String> maybeGetNamespace()
    {
        return namespaceSpec.optionalValue(getNodeGroup());
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return List.of(namespaceSpec, replicaSpec, imageSpec);
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(KubeControlProvider.class);
    }

    @Override
    public NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        boolean allReplicasPresent = nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(namespaceSpec.optionalValue(nodeGroup),
                namespacedKubeCtl -> namespacedKubeCtl.findPodNames(getPodLabelSelector()).size() ==
                    replicaSpec.value(nodeGroup));
        return allReplicasPresent ?
            NodeGroup.State.STARTED_SERVICES_RUNNING :
            NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        String deployment = renderDefinitionWithScopes(getDeploymentTemplate(), List.of(getTemplateParams()));
        Utils.writeStringToFile(getDeploymentArtifact().toFile(), deployment);

        return nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(namespaceSpec.optionalValue(nodeGroup), namespacedKubeCtl -> {
                String manifestContent = Utils.readStringFromFile(getDeploymentArtifact().toFile());
                boolean applied = applyAndWaitForManifest(nodeGroup, namespaceSpec.optionalValue(nodeGroup),
                    getDeploymentArtifact(), manifestContent, getWaitOptions());
                if (!applied)
                {
                    logger.error("Could not apply {}", name);
                    return false;
                }
                return postApply(namespacedKubeCtl);
            });
    }

    protected boolean postApply(KubeControlProvider.NamespacedKubeCtl namespacedKubeCtl)
    {
        return true;
    }

    private String getDeploymentTemplate()
    {
        return Exceptions.getUncheckedIO(() -> Resources.toString(
            Resources.getResource(KubernetesDeploymentConfigurationManager.class, template),
            StandardCharsets.UTF_8));
    }

    private Path getDeploymentArtifact()
    {
        return getNodeGroup().getLocalArtifactPath().resolve(String.format("%s_deployment.yaml", name));
    }

    private Map<String, Object> getTemplateParams()
    {
        return Map.of(
            "name", name(),
            "replicas", replicaSpec.value(getNodeGroup()),
            "image", imageSpec.value(getNodeGroup()));
    }

    private KubernetesManifestSpec.ManifestWaitOptions getWaitOptions()
    {
        return KubernetesManifestSpec.ManifestWaitOptions.containers(Duration.minutes(5), name(),
            ignored -> replicaSpec.value(getNodeGroup()));
    }

    @Override
    public boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(namespaceSpec.optionalValue(nodeGroup),
                namespacedKubeCtl -> namespacedKubeCtl.deleteResource(getDeploymentArtifact()).waitForSuccess());
    }

    protected Optional<Boolean> executeIfNodeHasPod(Node node, String action,
        BiFunction<KubeControlProvider.NamespacedKubeCtl, String, Boolean> f)
    {
        return node.getProvider(KubeControlProvider.class).inNamespace(namespaceSpec.optionalValue(node),
            namespacedKubeCtl -> {
                List<String> podNames = namespacedKubeCtl.findPodNames(getPodLabelSelector(), true);
                if (!podNames.isEmpty())
                {
                    return Optional.of(f.apply(namespacedKubeCtl, podNames.get(0)));
                }
                node.logger().info("No pod with label {} was found, will not {}", getPodLabelSelector(), action);
                return Optional.empty();
            });
    }

    /** A deployment where containers will not terminate without being deleted by running tail -f /dev/null
     * This allows an easy way of collecting artifacts from the container's volume.
     */
    public abstract static class WithPersistentContainer extends KubernetesDeploymentConfigurationManager
    {
        private static final String DEPLOYMENT_TEMPLATE = "eternal-deployment.yaml.mustache";

        private final Path podArtifactsDir;

        public WithPersistentContainer(String prefix, String name, String description, String imageBase)
        {
            super(prefix, name, description, imageBase, DEPLOYMENT_TEMPLATE);
            this.podArtifactsDir = Paths.get(String.format("/%s", name));
        }

        @Override
        public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
        {
            int numNodes = getNodeGroup().getNodes().size();
            int numReplica = super.replicaSpec.value(properties);
            if (numReplica > numNodes)
            {
                throw new PropertySpec.ValidationException(String.format(
                    "Cannot have more replica (%d) than nodes (%d)", numReplica, numNodes));
            }
        }

        protected Path getPodArtifactsDir()
        {
            return podArtifactsDir;
        }

        @Override
        protected boolean postApply(KubeControlProvider.NamespacedKubeCtl namespacedKubeCtl)
        {
            List<String> podNames = namespacedKubeCtl.findPodNames(getPodLabelSelector());
            if (podNames.isEmpty())
            {
                logger.error("No {} pods!", name());
                return false;
            }

            return Utils.waitForSuccess(getNodeGroup().logger(),
                podNames.stream()
                    .map(podName -> namespacedKubeCtl.makeDirs(name(), podName, podArtifactsDir.toString()))
                    .collect(Collectors.toList()));
        }

        @Override
        public boolean collectArtifactsImpl(Node node)
        {
            return executeIfNodeHasPod(node, String.format("collect %s artifacts", name()),
                (namespacedKubeCtl, podName) -> namespacedKubeCtl.copyFromContainer(
                    podName, name(), podArtifactsDir.toString(), node.getLocalArtifactPath().resolve(name()))
                    .waitForSuccess())
                        .orElse(true);
        }
    }
}
