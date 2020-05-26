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
package com.datastax.fallout.harness.modules.chaos_mesh;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
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
import com.datastax.fallout.ops.configmanagement.kubernetes.KubernetesManifestConfigurationManager;
import com.datastax.fallout.ops.providers.FileProvider;
import com.datastax.fallout.ops.providers.KubeControlProvider;
import com.datastax.fallout.ops.provisioner.kubernetes.KindProvisioner;
import com.datastax.fallout.util.Duration;

@AutoService(ConfigurationManager.class)
public class ChaosMeshConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.chaos_mesh.";

    private static final Path CHAOS_MESH_CRD = Paths.get(
        Resources.getResource(ChaosMeshConfigurationManager.class, "crd.yaml").getPath());
    private static final Path CHAOS_MESH_HELM_CHART = Paths.get(
        Resources.getResource(ChaosMeshConfigurationManager.class, "helm-chart").getPath());

    private static final Optional<String> NAMESPACE = Optional.of("chaos-mesh");

    private static final PropertySpec<String> optionsFileSpec =
        PropertySpecBuilder.createStr(prefix, FileProvider::validateIsManagedFile)
            .name("install_options_file")
            .description("Yaml file of options to pass to chaos mesh helm chart install")
            .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "chaos_mesh";
    }

    @Override
    public String description()
    {
        return "Deploys the chaos mesh CRD & operator.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("kubernetes/chaos-mesh");
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(optionsFileSpec)
            .build();
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup properties)
    {
        return Set.of(KubeControlProvider.class);
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup properties)
    {
        return Set.of(ChaosMeshProvider.class);
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        if (!deployChaosMeshCRD())
        {
            logger.error("Failed to deploy chaos mesh crd");
            return false;
        }

        Map<String, String> flags = nodeGroup.getProvisioner() instanceof KindProvisioner ?
            Map.of(
                "chaosDaemon.runtime", "containerd",
                "chaosDaemon.socketPath", "/run/containerd/containerd.sock") :
            Map.of();

        Optional<Path> optionsFile = optionsFileSpec.optionalValue(nodeGroup).map(
            marker -> nodeGroup.findFirstRequiredProvider(FileProvider.LocalFileProvider.class).getFullPath(marker));

        return nodeGroup.findFirstRequiredProvider(KubeControlProvider.class).inNamespace(NAMESPACE,
            namespacedKubeCtl -> namespacedKubeCtl.installHelmChart(CHAOS_MESH_HELM_CHART, flags, optionsFile));
    }

    private boolean deployChaosMeshCRD()
    {
        return KubernetesManifestConfigurationManager.applyAndWaitForManifest(getNodeGroup(), NAMESPACE, CHAOS_MESH_CRD,
            Utils.readStringFromFile(CHAOS_MESH_CRD.toFile()),
            KubernetesManifestSpec.ManifestWaitOptions.fixedDuration(Duration.seconds(5)));
    }

    @Override
    public boolean registerProviders(Node node)
    {
        new ChaosMeshProvider(node);
        return true;
    }

    @Override
    public boolean prepareArtifactsImpl(Node node)
    {
        return node.getProvider(KubeControlProvider.class).inNamespace(NAMESPACE,
            namespacedKubeCtl -> namespacedKubeCtl.executeIfNodeHasPod("collect chaos mesh logs",
                "app.kubernetes.io/component=controller-manager,app.kubernetes.io/name=chaos-mesh",
                (nsKctl, pod) -> {
                    nsKctl.captureContainerLogs(pod, Optional.empty(),
                        node.getLocalArtifactPath().resolve("chaos-mesh-controller.log"));
                    return true;
                }).orElse(true));
    }

    @Override
    public boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(NAMESPACE, namespacedKubeCtl -> namespacedKubeCtl.uninstallHelmChart(CHAOS_MESH_HELM_CHART) &&
                namespacedKubeCtl.deleteResource(CHAOS_MESH_CRD).waitForSuccess());
    }
}
