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
package com.datastax.fallout.harness.modules.nosqlbench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.specs.TimeoutSpec;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.providers.DataStaxCassOperatorProvider;
import com.datastax.fallout.ops.providers.KubeControlProvider;
import com.datastax.fallout.util.Duration;

@AutoService(Module.class)
public class NoSqlBenchModule extends Module
{
    private static final String prefix = "fallout.modules.nosqlbench.";

    private static final PropertySpec<String> targetGroupSpec = PropertySpecBuilder.serverGroup(prefix);

    private static final PropertySpec<Integer> numClientsSpecs = PropertySpecBuilder.createInt(prefix)
        .name("num_clients")
        .description("Number of nosqlbench clients to execute. Default is all replicas.")
        .build();

    private static final PropertySpec<Long> totalCyclesSpec = PropertySpecBuilder.createIterations(prefix)
        .name("cycles")
        .description(String.format("The total number of cycles to distribute across %s", numClientsSpecs.name()))
        .build();

    private static final PropertySpec<Long> cycleOffsetSpec = PropertySpecBuilder.createIterations(prefix)
        .name("cycles.offset")
        .description("Offset applied to the start of all client cycle ranges. If cycles is 50, offset is 10," +
            "and the number of clients is 5, the resulting ranges would be [(10, 20), (20, 30), (30, 40), " +
            "(40, 50), (50, 60)].")
        .defaultOf(0L)
        .build();

    private static final PropertySpec<List<String>> argsListSpec = PropertySpecBuilder.createStrList(prefix)
        .name("args")
        .description("List of arguments to pass into nosqlbench command.")
        .required()
        .build();

    private static final PropertySpec<Duration> histogramFrequencySpec = PropertySpecBuilder.createDuration(prefix)
        .name("histogram.frequency")
        .description("Frequency used for histogram logging. If number is 0, then histogram logging is disabled.")
        .defaultOf(Duration.fromString("5s"))
        .build();

    private static final TimeoutSpec timeoutSpec = new TimeoutSpec(prefix, Duration.hours(10));

    @Override
    public String name()
    {
        return "nosqlbench";
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String description()
    {
        return "Run a NoSqlBench kubernetes job.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("kubernetes/nosqlbench-workload.yaml");
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        return Set.of(NoSqlBenchProvider.class, DataStaxCassOperatorProvider.class, KubeControlProvider.class);
    }

    @Override
    public List<Product> getSupportedProducts()
    {
        return List.of(Product.DSE, Product.CASSANDRA);
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(targetGroupSpec, numClientsSpecs, totalCyclesSpec, cycleOffsetSpec, argsListSpec)
            .addAll(timeoutSpec.getSpecs())
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        Set<String> falloutManagedArgs = Set.of("host", "cycles", "--log-histograms", "--log-histostats", "-v");
        for (String arg : argsListSpec.value(properties))
        {
            for (String managedArg : falloutManagedArgs)
            {
                if (arg.strip().startsWith(managedArg))
                {
                    throw new PropertySpec.ValidationException(String.format(
                        "Please remove nosqlbench arg '%s' which conflicts with arg managed by Fallout", arg));
                }
            }
        }
    }

    private List<String> args = new ArrayList<>();
    private List<String> options = new ArrayList<>();

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        // separate arguments from options so we can encore correct order of each
        argsListSpec.optionalValue(properties).ifPresent(argsList -> argsList.forEach(arg -> {
            if (arg.strip().startsWith("-"))
            {
                options.add(arg);
            }
            else
            {
                args.add(arg);
            }
        }));
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        NodeGroup targetGroup = ensemble.getNodeGroupByAlias(targetGroupSpec.value(properties));

        List<NoSqlBenchProvider> nosqlBenchProviders = targetGroup.getNodes().stream()
            .map(n -> n.maybeGetProvider(NoSqlBenchProvider.class))
            .flatMap(Optional::stream)
            .collect(Collectors.toList());

        Path podArtifactsRoot = nosqlBenchProviders.get(0).getPodArtifactsDir();

        String contactPoint = targetGroup.findFirstRequiredProvider(DataStaxCassOperatorProvider.class)
            .getClusterService();
        args.add(String.format("host=%s", contactPoint));

        Duration histogramFrequency = histogramFrequencySpec.value(properties);
        if (histogramFrequency.value != 0)
        {
            Path histograms = podArtifactsRoot.resolve(String.format("%s.hdr", getInstanceName()));
            options.add(String.format("--log-histograms %s::%s", histograms, histogramFrequency.toAbbrevString()));

            Path histostats = podArtifactsRoot.resolve(String.format("%s.csv", getInstanceName()));
            options.add(String.format("--log-histostats %s::%s", histostats, histogramFrequency.toAbbrevString()));
        }

        if (!options.contains("-v"))
        {
            options.add("-v");
        }

        int numClients = numClientsSpecs.optionalValue(properties).orElse(nosqlBenchProviders.size());

        Long totalCycles = totalCyclesSpec.value(properties);
        Long cycleOffset = cycleOffsetSpec.value(properties);

        Optional<List<String>> cycleRanges = Optional.empty();
        if (totalCycles != null)
        {
            cycleRanges = Optional.of(buildCycleRangeByClientArgs(totalCycles, cycleOffset, numClients));
        }

        emitInvoke("Starting nosqlbench");

        List<NodeResponse> nosqlBenchCommands = new ArrayList<>();
        for (int i = 0; i < numClients; i++)
        {
            List<String> clientSpecificArgs = new ArrayList<>(args);
            if (cycleRanges.isPresent())
            {
                clientSpecificArgs.add(String.format("cycles=%s", cycleRanges.get().get(i)));
            }
            List<String> orderedCommandParameters =
                Stream.concat(clientSpecificArgs.stream(), options.stream()).collect(Collectors.toList());
            nosqlBenchCommands.add(nosqlBenchProviders.get(i).nosqlbench(getInstanceName(), orderedCommandParameters));
        }

        boolean success = Utils.waitForProcessEnd(logger, nosqlBenchCommands,
            wo -> wo.timeout = timeoutSpec.toDuration(properties));

        if (success)
        {
            emitOk("nosqlbench successful");
        }
        else
        {
            emitFail("nosqlbench failed");
        }
    }

    public static List<String> buildCycleRangeByClientArgs(long totalCycles, long cycleOffset, int numClients)
    {
        return distributeCycleRangeByClient(totalCycles, cycleOffset, numClients)
            .map(r -> String.format("%d..%d", r.lowerEndpoint(), r.upperEndpoint()))
            .collect(Collectors.toList());
    }

    @VisibleForTesting
    public static Stream<Range<Long>> distributeCycleRangeByClient(long totalCycles, long cycleOffset, int numClients)
    {
        Stream.Builder<Range<Long>> cycleRangeByClient = Stream.builder();

        final long cyclesPerClient = totalCycles / (long) numClients;
        final long clientsWith1AdditionalCycle = totalCycles % (long) numClients;

        long remainingCyclesSkew = 0;

        for (int i = 0; i < numClients; i++)
        {
            long start = (cyclesPerClient * i) + cycleOffset + remainingCyclesSkew;
            long end = start + cyclesPerClient;

            if (i < clientsWith1AdditionalCycle)
            {
                end += 1L;
                remainingCyclesSkew += 1;
            }

            cycleRangeByClient.add(Range.closedOpen(start, end));
        }
        return cycleRangeByClient.build();
    }
}
