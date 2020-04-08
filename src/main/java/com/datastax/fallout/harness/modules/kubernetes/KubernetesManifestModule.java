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
package com.datastax.fallout.harness.modules.kubernetes;

import java.util.List;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.specs.KubernetesManifestSpec;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.providers.FileProvider;
import com.datastax.fallout.ops.providers.KubeControlProvider;

import static com.datastax.fallout.ops.configmanagement.kubernetes.KubernetesManifestConfigurationManager.applyAndWaitForManifest;

@AutoService(Module.class)
public class KubernetesManifestModule extends Module
{
    private static final String prefix = "fallout.modules.kubernetes_manifest.";

    private static final KubernetesManifestSpec manifestSpec = new KubernetesManifestSpec(prefix);

    private static final PropertySpec<String> targetGroupSpec =
        PropertySpecBuilder.nodeGroup(prefix, "target_group", "NodeGroup to deploy the manifest on.", "server");

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "kubernetes_manifest";
    }

    @Override
    public String description()
    {
        return "Deploys a manifest file to the targeted kubernetes cluster.";
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        return ImmutableSet.of(KubeControlProvider.class, FileProvider.LocalFileProvider.class);
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .addAll(manifestSpec.getPropertySpecs())
            .add(targetGroupSpec)
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        manifestSpec.validateProperties(properties);
    }

    @Override
    public List<Product> getSupportedProducts()
    {
        return ImmutableList.of();
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        NodeGroup targetGroup = ensemble.getNodeGroupByAlias(targetGroupSpec.value(properties));
        emitInvoke("Applying manifest");
        boolean success = applyAndWaitForManifest(targetGroup, properties, manifestSpec);
        if (success)
        {
            emitOk("Manifest applied");
        }
        else
        {
            emitFail("Manifest was not applied");
        }
    }
}
