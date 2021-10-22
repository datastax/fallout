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
package com.datastax.fallout.components.kubernetes;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.ResourceUtils;

import static com.datastax.fallout.components.kubernetes.AbstractKubernetesProvisioner.DNS1123;
import static com.datastax.fallout.components.kubernetes.KubernetesManifestConfigurationManager.applyAndWaitForManifest;
import static com.datastax.fallout.util.MustacheFactoryWithoutHTMLEscaping.renderWithScopes;

/** Configuration Manager for Kubernetes Deployment objects based on templates kept as Fallout resource.
 *  Templating allows different replica and images, as well as handling naming and labels in a consistent manner.
 */
public abstract class KubernetesDeploymentConfigurationManager extends ConfigurationManager
{

    private final String prefix;
    private final String name;
    private final String description;
    private final String template;

    private final PropertySpec<String> namespaceSpec;
    private final PropertySpec<Integer> replicaSpec;
    private final PropertySpec<String> imageSpec;
    private final PropertySpec<String> cpuResourcesSpec;
    private final PropertySpec<Map<String, Object>> environmentVarsSpec;

    public KubernetesDeploymentConfigurationManager(String prefix, String name, String description,
        String imageDefault, String template)
    {
        Preconditions.checkArgument(DNS1123.matcher(name).matches(),
            "Implementations of KubernetesDeploymentConfigurationManager must use a name which is DNS1123 compliant");

        this.prefix = prefix;
        this.name = name;
        this.description = description;
        this.template = template;

        namespaceSpec = KubernetesDeploymentManifestSpec.buildNameSpaceSpec(prefix);
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
        cpuResourcesSpec = PropertySpecBuilder.createStr(prefix)
            .name("cpu")
            .description(
                "The number of cpu resources needed for each pod. https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu")
            .build();
        environmentVarsSpec = PropertySpecBuilder
            .<Object>createMap(prefix)
            .name("env")
            .description("Dictionary of environment variables to pass to each pod")
            .defaultOf(Collections.emptyMap())
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
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(namespaceSpec, replicaSpec, imageSpec, cpuResourcesSpec, environmentVarsSpec);
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
        String deployment = renderWithScopes(getDeploymentTemplate(), List.of(getTemplateParams()));
        FileUtils.writeString(getDeploymentArtifact(), deployment);

        return nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(namespaceSpec.optionalValue(nodeGroup), namespacedKubeCtl -> {
                String manifestContent = FileUtils.readString(getDeploymentArtifact());
                boolean applied = applyAndWaitForManifest(nodeGroup, namespaceSpec.optionalValue(nodeGroup),
                    getDeploymentArtifact(), manifestContent, getWaitOptions());
                if (!applied)
                {
                    logger().error("Could not apply {}", name);
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
        return ResourceUtils.readResourceAsString(KubernetesDeploymentConfigurationManager.class, template);
    }

    private Path getDeploymentArtifact()
    {
        return getNodeGroup().getLocalArtifactPath().resolve(String.format("%s_deployment.yaml", name));
    }

    protected List<Map<String, String>> getConfigMapVolumes()
    {
        return List.of();
    }

    protected List<Map<String, String>> getVolumeMounts()
    {
        return List.of();
    }

    private Map<String, Object> getTemplateParams()
    {
        Map<String, Object> v = new HashMap<>(Map.of(
            "name", name(),
            "replicas", replicaSpec.value(getNodeGroup()),
            "image", imageSpec.value(getNodeGroup()),
            "env", environmentVarsSpec.value(getNodeGroup()).entrySet(),
            "volumes", getConfigMapVolumes(),
            "volumeMounts", getVolumeMounts()));

        // Map.of does not allow null values, but cpu is allowed to be null, so we add directly to the HashMap
        v.put("cpu", cpuResourcesSpec.value(getNodeGroup()));

        return v;
    }

    private KubernetesDeploymentManifestSpec.ManifestWaitOptions getWaitOptions()
    {
        return KubernetesDeploymentManifestSpec.ManifestWaitOptions.containers(Duration.minutes(5), name(),
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
            namespacedKubeCtl -> namespacedKubeCtl.executeIfNodeHasPod(action, getPodLabelSelector(), f));
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
                logger().error("No {} pods!", name());
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
