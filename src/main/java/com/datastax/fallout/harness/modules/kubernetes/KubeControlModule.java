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
import com.datastax.fallout.harness.modules.RepeatableNodeCommandModule;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.providers.KubeControlProvider;

import static com.datastax.fallout.harness.specs.KubernetesManifestSpec.buildNameSpaceSpec;

@AutoService(Module.class)
public class KubeControlModule extends RepeatableNodeCommandModule<KubeControlProvider>
{
    private static final String prefix = "fallout.module.kubectl.";

    private static final PropertySpec<String> nameSpaceSpec = buildNameSpaceSpec(prefix);

    public KubeControlModule()
    {
        super(KubeControlProvider.class, prefix);
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "kubectl";
    }

    @Override
    public String description()
    {
        return "Executes kubectl commands.";
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        return ImmutableSet.of(KubeControlProvider.class);
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .addAll(super.getModulePropertySpecs())
            .add(nameSpaceSpec).build();
    }

    @Override
    public List<Product> getSupportedProducts()
    {
        return ImmutableList.of();
    }

    @Override
    protected String commandDescription()
    {
        return "kubectl command to execute. Exclude 'kubectl' from command.";
    }

    @Override
    protected NodeResponse runCommand(KubeControlProvider kubeCtlProvider, String command)
    {
        return kubeCtlProvider.inNamespace(nameSpaceSpec.optionalValue(properties),
            kubeCtl -> kubeCtl.execute(command));
    }
}
