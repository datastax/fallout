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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.ProviderUtil;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Exceptions;

@AutoService(ConfigurationManager.class)
public class KubernetesManifestConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.kubernetes.generic.";

    private final KubernetesDeploymentManifestSpec manifestSpec =
        new KubernetesDeploymentManifestSpec(prefix, this::prefix);

    private final ProviderUtil.DynamicProviderSpec providerSpec =
        new ProviderUtil.DynamicProviderSpec(prefix, this::prefix);

    @Override
    public String prefix()
    {
        //Let's this CM be used multiple times in the same nodeGroup
        return getInstanceName() != null ? prefix + getInstanceName() + "." : prefix;
    }

    @Override
    public String name()
    {
        return "kubernetes_manifest";
    }

    @Override
    public String description()
    {
        return "Configuration manager for applying a manifest file to a kubernetes cluster.";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .addAll(manifestSpec.getPropertySpecs())
            .addAll(providerSpec.getSpecs())
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        manifestSpec.validateProperties(properties);
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(KubeControlProvider.class, FileProvider.class);
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return providerSpec.getAvailableDynamicProviders(nodeGroupProperties);
    }

    @Override
    public boolean registerProviders(Node node)
    {
        return providerSpec.registerDynamicProvider(node, getNodeGroup().getProperties(), logger());
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        return applyAndWaitForManifest(nodeGroup, manifestSpec);
    }

    @Override
    public boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return deleteResourcesFromManifest(nodeGroup, manifestSpec);
    }

    public static boolean applyAndWaitForManifest(NodeGroup nodeGroup,
        KubernetesDeploymentManifestSpec manifestSpec)
    {
        return applyAndWaitForManifest(nodeGroup, nodeGroup.getProperties(), manifestSpec);
    }

    public static boolean applyAndWaitForManifest(NodeGroup nodeGroup, PropertyGroup properties,
        KubernetesDeploymentManifestSpec manifestSpec)
    {
        return applyAndWaitForManifest(nodeGroup,
            manifestSpec.maybeGetNameSpace(properties),
            manifestSpec.getManifestArtifactPath(nodeGroup, properties),
            manifestSpec.getManifestContent(nodeGroup, properties),
            manifestSpec.getManifestWaitOptions(properties));
    }

    public static boolean applyAndWaitForManifest(NodeGroup nodeGroup, Optional<String> namespace,
        Path manifestArtifact, String manifestContent,
        KubernetesDeploymentManifestSpec.ManifestWaitOptions waitOptions)
    {
        return nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(namespace, namespacedKubeCtl -> {
                NodeResponse apply = namespacedKubeCtl.applyManifest(manifestArtifact);
                if (!apply.waitForSuccess())
                {
                    nodeGroup.logger().error(String.format("Applying manifest %s failed!",
                        manifestArtifact.getFileName()));
                    return false;
                }
                return namespacedKubeCtl.waitForManifest(manifestArtifact, manifestContent, waitOptions);
            });
    }

    public static boolean deleteResourcesFromManifest(NodeGroup nodeGroup,
        KubernetesDeploymentManifestSpec manifestSpec)
    {
        return deleteResourcesFromManifest(nodeGroup, nodeGroup.getProperties(), manifestSpec);
    }

    public static boolean deleteResourcesFromManifest(NodeGroup nodeGroup, PropertyGroup properties,
        KubernetesDeploymentManifestSpec manifestSpec)
    {
        boolean deleted = nodeGroup.findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(manifestSpec.maybeGetNameSpace(properties),
                namespacedKubeCtl -> namespacedKubeCtl
                    .deleteResource(manifestSpec.getManifestArtifactPath(nodeGroup, properties))
                    .waitForSuccess());
        if (!deleted)
        {
            return false;
        }
        // A one minute grace period for garbage collection. This is important during teardown, without which dependants
        // of deleted objects may not yet be deleted before Fallout begins to destroy the cluster. See FAL-1528.
        Exceptions.runUnchecked(() -> Thread.sleep(60000));
        return true;
    }
}
