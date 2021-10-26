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
package com.datastax.fallout.components.flamegraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;

/**
 * Module that runs and generates flamegraphs
 */
@AutoService(Module.class)
public class FlamegraphModule extends Module
{

    private static final String prefix = "fallout.module.flamegraph.";

    static final PropertySpec<Integer> delaySecondsSpec = PropertySpecBuilder.createInt(prefix)
        .name("time.delay.seconds")
        .description("The number of seconds to delay before starting")
        .defaultOf(15)
        .build();

    static final PropertySpec<Integer> runSecondsSpec = PropertySpecBuilder.createInt(prefix)
        .name("time.capture.seconds")
        .description("The number of seconds to profile the process")
        .defaultOf(15)
        .build();

    static final PropertySpec<String> processNameSpec = PropertySpecBuilder.createStr(prefix)
        .name("process.name")
        .description("The name of the process to profile (can be a partial name)")
        .build();

    static final PropertySpec<Integer> processPortSpec = PropertySpecBuilder.createInt(prefix)
        .name("process.port")
        .description("The port number of process to profile")
        .build();

    static final PropertySpec<Boolean> showLibrarySourceSpec = PropertySpecBuilder.createBool(prefix, false)
        .name("include.library.sources")
        .description("Include the source lines of any shared libraries (very slow)")
        .build();

    static final PropertySpec<String> eventNameSpec = PropertySpecBuilder.createStr(prefix)
        .name("event")
        .description("The name of the event you wish to record (see perf list)")
        .defaultOf("cycles:p").build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "flamegraph";
    }

    @Override
    public String description()
    {
        return "Module that generates flamegraphs of running processes";
    }

    @Override
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return List.of(processNameSpec,
            delaySecondsSpec,
            runSecondsSpec,
            processPortSpec,
            eventNameSpec,
            showLibrarySourceSpec);
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        validator.ensembleRequiresProvider(FlamegraphProvider.class);
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        try
        {
            Integer delaySecs = delaySecondsSpec.value(properties);
            Integer runSecs = runSecondsSpec.value(properties);
            String name = processNameSpec.value(properties);
            Integer port = processPortSpec.value(properties);

            if (name == null && port == null)
            {
                emitError("No process name or port passed in module properties");
                return;
            }

            List<FlamegraphProvider> providerList = ensemble.getUniqueNodeGroupInstances().stream()
                .flatMap(g -> g.getNodes().stream())
                .map(n -> n.maybeGetProvider(FlamegraphProvider.class))
                .flatMap(Optional::stream)
                .collect(Collectors.toList());
            if (providerList.isEmpty())
            {
                emitError("No Flamegraph providers found");
                return;
            }

            //Create processId getter
            String processIdGetter;
            if (name != null)
            {
                processIdGetter = String.format("`pidof -s %s`", name);
            }
            else
            {
                processIdGetter = String
                    .format("`netstat -tunapl | grep :%d | grep LISTEN | awk '{print $7}' | sed 's!/.*!!'`", port);
            }

            emitInfo(String.format("Delaying start for %d seconds", delaySecs));
            Uninterruptibles.sleepUninterruptibly(delaySecs, TimeUnit.SECONDS);

            emitInvoke(String.format("Starting flamegraph recording of %s for %d seconds", name, runSecs));
            List<NodeResponse> responses = new ArrayList<>(providerList.size());
            for (FlamegraphProvider f : providerList)
            {
                responses.add(f.collectProfileData(processIdGetter, eventNameSpec.value(properties), runSecs,
                    getInstanceName(), showLibrarySourceSpec.value(properties)));
            }

            if (!Utils.waitForSuccess(logger(), responses, Duration.hours(3)))
            {
                emitError(
                    "Flamegraph capture failed or timed out.  If the target node's kernel is < 4.10, you should expect intermittent failures: see FAL-857");
                return;
            }
            emitOk(String.format("Flamegraph capture of %s for %d seconds complete", name, runSecs));
        }
        catch (Throwable t)
        {
            emit(Operation.Type.error, t);
        }
    }
}
