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

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.specs.KubernetesManifestSpec;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.providers.DataStaxCassOperatorProvider;
import com.datastax.fallout.ops.providers.FileProvider;
import com.datastax.fallout.ops.providers.KubeControlProvider;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.YamlUtils;

import static com.datastax.fallout.harness.specs.KubernetesManifestSpec.buildNameSpaceSpec;
import static com.datastax.fallout.ops.configmanagement.kubernetes.KubernetesManifestConfigurationManager.applyAndWaitForManifest;
import static com.datastax.fallout.ops.configmanagement.kubernetes.KubernetesManifestConfigurationManager.deleteResourcesFromManifest;

@AutoService(ConfigurationManager.class)
public class DataStaxCassOperatorConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.ds_cass_operator.";

    private static final PropertySpec<String> nameSpaceSpec = buildNameSpaceSpec(prefix);

    private static final KubernetesManifestSpec operatorManifestSpec =
        new KubernetesManifestSpec(prefix, "operator", nameSpaceSpec, true,
            getWaitOptions(DataStaxCassOperatorConfigurationManager::getOperatorImage));

    private static final KubernetesManifestSpec datacenterManifestSpec =
        new KubernetesManifestSpec(prefix, "datacenter", nameSpaceSpec, false,
            getWaitOptions(DataStaxCassOperatorConfigurationManager::getServerImage));

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "ds_cass_operator";
    }

    @Override
    public String description()
    {
        return "Deploys the DataStax Cass Operator";
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(nameSpaceSpec)
            .addAll(operatorManifestSpec.getPropertySpecs())
            .addAll(datacenterManifestSpec.getPropertySpecs())
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        operatorManifestSpec.validateProperties(properties);
        if (datacenterManifestSpec.isPresent(properties))
        {
            datacenterManifestSpec.validateProperties(properties);
        }
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        Set<Class<? extends Provider>> available = new HashSet<>();
        if (datacenterManifestSpec.isPresent(nodeGroupProperties))
        {
            available.add(DataStaxCassOperatorProvider.class);
        }
        return available;
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        return ImmutableSet.of(KubeControlProvider.class, FileProvider.class);
    }

    private static KubernetesManifestSpec.ManifestWaitOptions getWaitOptions(Function<String, String> imageName)
    {
        return KubernetesManifestSpec.ManifestWaitOptions.image(Duration.minutes(20), "condition=ready", imageName);
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        return applyAndWaitForManifest(nodeGroup, operatorManifestSpec);
    }

    @Override
    public boolean registerProviders(Node node)
    {
        if (datacenterManifestSpec.isPresent(getNodeGroup().getProperties()))
        {
            new DataStaxCassOperatorProvider(node,
                getClusterService(datacenterManifestSpec.getManifestContent(getNodeGroup())));
        }
        return true;
    }

    @Override
    public boolean startImpl(NodeGroup nodeGroup)
    {
        if (datacenterManifestSpec.isPresent(nodeGroup.getProperties()))
        {
            return applyAndWaitForManifest(nodeGroup, datacenterManifestSpec);
        }
        return true;
    }

    @Override
    public boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        boolean clusterDeleted = true;
        if (datacenterManifestSpec.isPresent(nodeGroup.getProperties()))
        {
            clusterDeleted = deleteResourcesFromManifest(nodeGroup, datacenterManifestSpec);
        }
        return deleteResourcesFromManifest(nodeGroup, operatorManifestSpec) &&
            clusterDeleted;
    }

    @Override
    public boolean prepareArtifactsImpl(Node node)
    {
        return node.getProvider(KubeControlProvider.class).inNamespace(nameSpaceSpec.optionalValue(node),
            namespacedKubeCtl -> {
                Set<String> podNames = new HashSet<>();

                String operatorImage = getOperatorImage(operatorManifestSpec.getManifestContent(getNodeGroup()));
                podNames.addAll(namespacedKubeCtl.findPodNamesRunningImage(operatorImage, true));

                String serverImage = getServerImage(datacenterManifestSpec.getManifestContent(getNodeGroup()));
                podNames.addAll(namespacedKubeCtl.findPodNamesRunningImage(serverImage, true));

                for (String podName : podNames)
                {
                    List<String> containers = namespacedKubeCtl.getAllContainersInPod(podName);
                    for (String container : containers)
                    {
                        Path logArtifact = node.getLocalArtifactPath().resolve(
                            String.format("%s_%s_container.log", podName, container));
                        namespacedKubeCtl.captureContainerLogs(podName, Optional.of(container), logArtifact);
                    }
                }
                return true;
            });
    }

    @Override
    public NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(nameSpaceSpec.optionalValue(nodeGroup), namespacedKubeCtl -> {
                String operatorManifest = operatorManifestSpec.getManifestContent(nodeGroup);
                if (namespacedKubeCtl.findPodNamesRunningImage(getOperatorImage(operatorManifest)).size() == 0)
                {
                    return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
                }
                if (datacenterManifestSpec.isPresent(nodeGroup.getProperties()))
                {
                    String clusterManifest = datacenterManifestSpec.getManifestContent(nodeGroup);
                    if (namespacedKubeCtl.findPodNamesRunningImage(getServerImage(clusterManifest)).size() == 0)
                    {
                        return NodeGroup.State.STARTED_SERVICES_CONFIGURED;
                    }
                }
                return NodeGroup.State.STARTED_SERVICES_RUNNING;
            });
    }

    @VisibleForTesting
    public static String getServerImage(String clusterManifest)
    {
        JsonNode serverImage = YamlUtils.loadYamlDocument(clusterManifest, "/spec/serverImage");
        if (serverImage.isMissingNode())
        {
            throw new RuntimeException("serverImage is required to be present in the CassandraDatacenter definition");
        }
        return serverImage.asText();
    }

    @VisibleForTesting
    public static String getClusterService(String clusterManifest)
    {
        JsonNode cluster = YamlUtils.loadYamlDocument(clusterManifest);
        JsonNode clusterName = cluster.at("/spec/clusterName");
        JsonNode datacenter = cluster.at("/metadata/name");
        if (clusterName.isMissingNode() || datacenter.isMissingNode())
        {
            throw new RuntimeException(
                "clusterName and datacenter name are required in the CassandraDatacenter definition");
        }
        return String.format("%s-%s-service", clusterName.asText(), datacenter.asText());
    }

    @VisibleForTesting
    public static String getOperatorImage(String operatorManifest)
    {
        for (JsonNode document : YamlUtils.loadYamlDocuments(operatorManifest))
        {
            if (document.at("/kind").asText().equals("Deployment"))
            {
                JsonNode image = document.at("/spec/template/spec/containers/0/image");
                if (image.isMissingNode())
                {
                    break;
                }
                return image.asText();
            }
        }
        throw new RuntimeException("Could not get operator image from manifest");
    }
}
