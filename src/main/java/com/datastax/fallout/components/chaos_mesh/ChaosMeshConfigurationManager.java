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
package com.datastax.fallout.components.chaos_mesh;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

import com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec;
import com.datastax.fallout.components.kubernetes.KindProvisioner;
import com.datastax.fallout.components.kubernetes.KubeControlProvider;
import com.datastax.fallout.components.kubernetes.KubernetesManifestConfigurationManager;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.ResourceUtils;

import static com.datastax.fallout.components.kubernetes.HelmChartConfigurationManager.buildHelmInstallDebugSpec;
import static com.datastax.fallout.components.kubernetes.HelmChartConfigurationManager.buildHelmInstallDependencyUpdateSpec;
import static com.datastax.fallout.components.kubernetes.HelmChartConfigurationManager.buildHelmValuesFileSpec;

@AutoService(ConfigurationManager.class)
public class ChaosMeshConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.chaos_mesh.";

    private static final String CHAOS_MESH_CRD = "crd.yaml";
    private static final String CHAOS_MESH_HELM_CHART = "helm-chart";

    private static final String RELEASE_NAME = "chaos-mesh";
    private static final Optional<String> NAMESPACE = Optional.of(RELEASE_NAME);

    private static final PropertySpec<String> optionsFileSpec =
        buildHelmValuesFileSpec(prefix);
    private final PropertySpec<Boolean> installDebugSpec = buildHelmInstallDebugSpec(this::prefix);
    private final PropertySpec<Boolean> installDependencyUpdateSpec =
        buildHelmInstallDependencyUpdateSpec(this::prefix);

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
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(optionsFileSpec, installDebugSpec, installDependencyUpdateSpec);
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

    private Path configFilePath(String path)
    {
        return getNodeGroup().getLocalArtifactPath().resolve(RELEASE_NAME).resolve(path);
    }

    private void writeConfigFiles(String rootPath)
    {
        ResourceUtils.walkResourceTree(this.getClass(), rootPath, (path, content) -> Exceptions.runUncheckedIO(() -> {
            Files.createDirectories(configFilePath(path).getParent());
            Files.copy(content, configFilePath(path));
        }));
    }

    private boolean inNamespace(Function<KubeControlProvider.NamespacedKubeCtl, Boolean> function)
    {
        return getNodeGroup().findFirstRequiredProvider(KubeControlProvider.class).inNamespace(NAMESPACE, function);
    }

    @Override
    public NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return inNamespace(namespacedKubeCtl -> namespacedKubeCtl.checkHelmChartDeployed(RELEASE_NAME)) ?
            NodeGroup.State.STARTED_SERVICES_RUNNING :
            NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        writeConfigFiles(CHAOS_MESH_CRD);
        writeConfigFiles(CHAOS_MESH_HELM_CHART);

        if (!deployChaosMeshCRD())
        {
            logger().error("Failed to deploy chaos mesh crd");
            return false;
        }

        final List<String> setValues = nodeGroup.getProvisioner() instanceof KindProvisioner ?
            List.of(
                "chaosDaemon.runtime=containerd",
                "chaosDaemon.socketPath=/run/containerd/containerd.sock"
            ) :
            List.of();

        return inNamespace(namespacedKubeCtl -> namespacedKubeCtl.installHelmChart(
            configFilePath(CHAOS_MESH_HELM_CHART),
            KubeControlProvider.HelmInstallValues.of(
                optionsFileSpec.optionalValue(nodeGroup).stream().collect(Collectors.toList()),
                setValues),
            installDebugSpec.value(nodeGroup),
            Duration.minutes(5),
            installDependencyUpdateSpec.value(nodeGroup),
            Optional.empty()));
    }

    private boolean deployChaosMeshCRD()
    {
        return KubernetesManifestConfigurationManager.applyAndWaitForManifest(getNodeGroup(), NAMESPACE,
            configFilePath(CHAOS_MESH_CRD),
            FileUtils.readString(configFilePath(CHAOS_MESH_CRD)),
            KubernetesDeploymentManifestSpec.ManifestWaitOptions.fixedDuration(Duration.seconds(5)));
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
        return inNamespace(namespacedKubeCtl -> namespacedKubeCtl.uninstallHelmChart(
            configFilePath(CHAOS_MESH_HELM_CHART)) &&
            namespacedKubeCtl.deleteResource(configFilePath(CHAOS_MESH_CRD)).waitForSuccess());
    }
}
