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

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.jar.JarFile;

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
import com.datastax.fallout.util.Exceptions;

@AutoService(ConfigurationManager.class)
public class ChaosMeshConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.chaos_mesh.";

    private static final String CHAOS_MESH_CRD = "crd.yaml";
    private static final String CHAOS_MESH_HELM_CHART = "helm-chart";

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

    private Path configFilePath(String path)
    {
        return getNodeGroup().getLocalArtifactPath().resolve("chaos-mesh").resolve(path);
    }

    private void writeConfigFiles(String rootPath)
    {
        walkResourceTree(this.getClass(), rootPath, (path, content) -> Exceptions.runUncheckedIO(() -> {
            Files.createDirectories(configFilePath(path).getParent());
            Files.write(configFilePath(path), content);
        }));
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        writeConfigFiles(CHAOS_MESH_CRD);
        writeConfigFiles(CHAOS_MESH_HELM_CHART);

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
            namespacedKubeCtl -> namespacedKubeCtl.installHelmChart(configFilePath(CHAOS_MESH_HELM_CHART), flags,
                optionsFile));
    }

    private boolean deployChaosMeshCRD()
    {
        return KubernetesManifestConfigurationManager.applyAndWaitForManifest(getNodeGroup(), NAMESPACE,
            configFilePath(CHAOS_MESH_CRD),
            Utils.readStringFromFile(configFilePath(CHAOS_MESH_CRD).toFile()),
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
            .inNamespace(NAMESPACE, namespacedKubeCtl -> namespacedKubeCtl.uninstallHelmChart(
                configFilePath(CHAOS_MESH_HELM_CHART)) &&
                namespacedKubeCtl.deleteResource(configFilePath(CHAOS_MESH_CRD)).waitForSuccess());
    }

    private static void walkJarResourceTree(String path, URL resourceUrl, Consumer<String> pathConsumer)
    {
        final var jarAndResourcePath = resourceUrl.getPath().split("!", 2);
        final var jarPath = jarAndResourcePath[0].substring(5);
        final var resourcePath = jarAndResourcePath[1].substring(1);

        final var jarFile = Exceptions.getUncheckedIO(() -> new JarFile(new File(jarPath)));
        final var entries = jarFile.entries();

        while (entries.hasMoreElements())
        {
            final var entry = entries.nextElement();
            final var entryName = entry.getName();
            if (!entry.isDirectory() && entryName.startsWith(resourcePath))
            {
                final var entryPath = path + entryName.substring(resourcePath.length());
                pathConsumer.accept(entryPath);
            }
        }

    }

    private static void walkFileResourceTree(String path, File resourceFile, Consumer<String> pathConsumer)
    {
        if (resourceFile.isDirectory())
        {
            for (var file : resourceFile.listFiles())
            {
                walkFileResourceTree(path + "/" + file.getName(), file, pathConsumer);
            }
        }
        else
        {
            pathConsumer.accept(path);
        }
    }

    private static void walkResourceTree(Class<?> clazz, String path, BiConsumer<String, byte[]> pathAndContentConsumer)
    {
        final Consumer<String> pathConsumer = path_ -> pathAndContentConsumer.accept(path_,
            Exceptions.getUncheckedIO(() -> Resources.toByteArray(clazz.getResource(path_))));

        final var resourceUrl = clazz.getResource(path);

        if (resourceUrl.getProtocol().equals("jar"))
        {
            walkJarResourceTree(path, resourceUrl, pathConsumer);
        }
        else
        {
            walkFileResourceTree(path, new File(Exceptions.getUnchecked(() -> resourceUrl.toURI())), pathConsumer);
        }
    }

    public static void main(String[] args)
    {
        walkResourceTree(ChaosMeshConfigurationManager.class, "crd.yaml",
            (path, content) -> System.out.println(">>> " + path + "\n" + new String(content, StandardCharsets.UTF_8)));
        walkResourceTree(ChaosMeshConfigurationManager.class, "helm-chart",
            (path, content) -> System.out.println(">>> " + path + "\n" + new String(content, StandardCharsets.UTF_8)));
    }
}
