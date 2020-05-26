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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.providers.FileProvider;
import com.datastax.fallout.ops.providers.KubeControlProvider;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.harness.specs.KubernetesManifestSpec.buildNameSpaceSpec;

@AutoService(Module.class)
public class ChaosMeshModule extends Module
{
    private static final String prefix = "fallout.modules.chaos_mesh.";

    private static final PropertySpec<String> targetGroupSpec =
        PropertySpecBuilder.nodeGroup(prefix, "target_group", "NodeGroup to run the experiment in.", "server");

    private static final PropertySpec<String> namespaceSpec = buildNameSpaceSpec(prefix);

    private static final PropertySpec<String> experimentSpec =
        PropertySpecBuilder.createStr(prefix, FileProvider::validateIsManagedFile)
            .name("experiment")
            .description("Yaml containing experiment to execute")
            .required()
            .build();

    private static final PropertySpec<Duration> experimentDurationSpec = PropertySpecBuilder.createDuration(prefix)
        .name("experiment.duration")
        .description("The length of time to run the experiment for")
        .defaultOf(Duration.minutes(5))
        .build();

    public ChaosMeshModule()
    {
        super(RunToEndOfPhaseMethod.MANUAL, Lifetime.RUN_TO_END_OF_PHASE);
    }

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
        return "Runs a chaos mesh experiment.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("kubernetes/chaos-mesh");
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(namespaceSpec, experimentSpec, targetGroupSpec, experimentDurationSpec)
            .build();
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        return Set.of(KubeControlProvider.class, ChaosMeshProvider.class);
    }

    @Override
    public List<Product> getSupportedProducts()
    {
        return Product.everything();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        if (!runsToEndOfPhase() && experimentDurationSpec.optionalValue(properties).isEmpty())
        {
            throw new PropertySpec.ValidationException(String.format("%s must be set if module lifetime is %s",
                experimentDurationSpec.name(), Lifetime.RUN_ONCE));
        }
    }

    private NodeGroup targetGroup;
    private Optional<String> namespace;
    private ChaosMeshProvider chaosMesh;
    private Path experiment;

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        targetGroup = ensemble.getNodeGroupByAlias(targetGroupSpec.value(properties));
        namespace = namespaceSpec.optionalValue(properties);
        chaosMesh = targetGroup.findFirstRequiredProvider(ChaosMeshProvider.class);
        experiment = targetGroup.findFirstRequiredProvider(FileProvider.LocalFileProvider.class)
            .getFullPath(experimentSpec.value(properties));
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        emitInvoke("Starting chaos mesh experiment");
        if (chaosMesh.startExperiment(experiment, namespace).waitForSuccess())
        {
            emitInfo("Chaos mesh experiment deployed");
        }
        else
        {
            emitError("Chaos mesh experiment failed to deploy");
            return;
        }

        if (runsToEndOfPhase())
        {
            Uninterruptibles.awaitUninterruptibly(getUnfinishedRunOnceModules());
        }
        else
        {
            // always false condition to block for the experiment duration or until abort
            Utils.AwaitConditionOptions awaitOptions = new Utils.AwaitConditionOptions(logger(), () -> false,
                experimentDurationSpec.value(properties), timer);
            awaitOptions.addTestRunAbortTimeout(targetGroup);
            Utils.awaitConditionAsync(awaitOptions).join();
        }

        if (!chaosMesh.stopExperiment(experiment, namespace).waitForSuccess())
        {
            throw new RuntimeException("Failed to delete chaos mesh experiment");
        }

        emitOk("Chaos mesh experiment finished");
    }
}
