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
package com.datastax.fallout.components.nosqlbench;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.common.provider.ServiceContactPointProvider;
import com.datastax.fallout.components.common.spec.NodeSelectionSpec;
import com.datastax.fallout.components.common.spec.TimeoutSpec;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;

@AutoService(Module.class)
public class NoSqlBenchModule extends Module
{
    private static final String prefix = "fallout.modules.nosqlbench.";
    private static final Predicate<String> NB_VERBOSITY_ARG = Pattern.compile("-[v]{1,3}").asMatchPredicate();
    private static final String NB_STACKTRACES_ARG = "--show-stacktraces";
    private static final Set<String> FALLOUT_MANAGED_ARGS = Set.of(
        "host", "cycles", "alias", "run", "start", "--log-histograms", "--log-histostats", "--logs-dir");

    private static final PropertySpec<Integer> numClientsSpecs = PropertySpecBuilder.createInt(prefix)
        .name("num_clients")
        .description("Number of nosqlbench clients to execute. Default is all replicas.")
        .build();

    private static final NodeSelectionSpec serverGroupSpec = new NodeSelectionSpec(prefix, "server", true, false);
    private static final NodeSelectionSpec clientGroupSpec = new NodeSelectionSpec(prefix, "client", true, true);

    private static final PropertySpec<String> serviceTypeSpec = PropertySpecBuilder.createStr(prefix)
        .name("service_type")
        .description(
            "The type of service to connect to, correlates to the service described by the ServiceContactProvider.")
        .defaultOf("cassandra")
        .build();

    private static final PropertySpec<Long> totalCyclesSpec = PropertySpecBuilder.createIterations(prefix)
        .name("cycles")
        .description("The total number of cycles to distribute across the clients")
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
        .disableRefExpansion()
        .build();

    private static final PropertySpec<Duration> histogramFrequencySpec = PropertySpecBuilder.createDuration(prefix)
        .name("histogram.frequency")
        .description("Frequency used for histogram logging. If number is 0, then histogram logging is disabled.")
        .defaultOf(Duration.fromString("5s"))
        .build();

    private static final PropertySpec<Boolean> syncStartSpec = PropertySpecBuilder.createBool(prefix, false)
        .name("sync.start")
        .description("When running with many clients, synchronize the start of the job across clients")
        .build();

    private static final PropertySpec<String> hostParamSpec = PropertySpecBuilder.createStr(prefix)
        .name("host")
        .description(
            "Host argument in its entirety, e.g. host=10.0.0.1 or secureconnectbundle=path/to/bundle.zip Note: environment variables are not available.")
        .build();

    private static final TimeoutSpec timeoutSpec = new TimeoutSpec(prefix, Duration.hours(10));

    private static final PropertySpec<Duration> workloadDurationSpec = PropertySpecBuilder.createDuration(prefix)
        .name("duration")
        .description(
            "Duration for which the workload will be run. If set, make sure cycles is set high enough to supply statements for the entire duration of the workload.")
        .build();

    private static final PropertySpec<String> aliasSpec = PropertySpecBuilder.createStr(prefix)
        .name("alias")
        .description("Sets an explicit alias. Default is the module instance name.")
        .build();

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
        return "Run NoSqlBench clients";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("workloads/nosqlbench/nosqlbench-example.yaml");
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        clientPods =
            validator.nodeGroupWillHaveProvider(clientGroupSpec.getNodeGroupSpec(), NoSqlBenchPodProvider.class);
        if (clientPods)
        {
            // client pods and server pods have to run on the same k8s cluster nodegroup
            Optional<NodeGroup> clientGroup = validator.getNodeGroup(clientGroupSpec.getNodeGroupSpec());
            Optional<NodeGroup> serverGroup = validator.getNodeGroup(serverGroupSpec.getNodeGroupSpec());
            if (clientGroup.isEmpty() || serverGroup.isEmpty() || !clientGroup.get().equals(serverGroup.get()))
            {
                validator.addValidationError("client and server groups must be the same nodegroup");
            }
            validator.nodeGroupRequiresProvider(clientGroupSpec, NoSqlBenchPodProvider.class);
        }
        else
        {
            validator.nodeGroupRequiresProvider(clientGroupSpec, NoSqlBenchProvider.class);
        }

        if (hostParamSpec.optionalValue(getProperties()).isEmpty())
        {
            validator.nodeGroupRequiresProvider(clientPods ? clientGroupSpec : serverGroupSpec,
                ServiceContactPointProvider.class);
        }
    }

    @Override
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(numClientsSpecs, totalCyclesSpec, cycleOffsetSpec, argsListSpec, histogramFrequencySpec,
                syncStartSpec, workloadDurationSpec, aliasSpec, hostParamSpec, serviceTypeSpec)
            .addAll(serverGroupSpec.getSpecs())
            .addAll(clientGroupSpec.getSpecs())
            .addAll(timeoutSpec.getSpecs())
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        for (String arg : argsListSpec.value(properties))
        {
            for (String managedArg : FALLOUT_MANAGED_ARGS)
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
    private String alias;
    private NodeGroup clientGroup;
    private boolean clientPods;

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        args.add(workloadDurationSpec.optionalValue(properties).isPresent() ? "start" : "run");
        alias = aliasSpec.optionalValue(properties).orElseGet(this::getInstanceName);
        args.add(String.format("alias=%s", alias));

        clientGroup = ensemble.getNodeGroupByAlias(clientGroupSpec.getNodeGroupSpec().value(properties));
        Optional<FileProvider.RemoteFileProvider> remoteFileProvider = clientGroup.findFirstProvider(
            FileProvider.RemoteFileProvider.class);

        // separate arguments from options so we can enforce correct order of each
        argsListSpec.optionalValue(properties).ifPresent(argsList -> argsList.stream()
            .map(arg -> remoteFileProvider.map(p -> p.expandRefs(arg)).orElse(arg))
            .forEach(arg -> {
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
        List<NoSqlBenchProvider> nosqlBenchProviders = null;
        int numClients = -1;
        String clientPrepareScript = "";
        if (clientPods)
        {
            nosqlBenchProviders = clientGroup.getNodes().stream()
                .map(n -> n.maybeGetProvider(NoSqlBenchPodProvider.class))
                .flatMap(Optional::stream)
                .collect(Collectors.toList());
            numClients = numClientsSpecs.optionalValue(properties).orElse(nosqlBenchProviders.size());

            boolean syncStart = syncStartSpec.value(properties) && numClients > 1;
            if (syncStart)
            {
                String syncId = UUID.randomUUID().toString();
                clientPrepareScript = String.format("curl -sf http://countdownlatch.org/%d/%s && ", numClients, syncId);
            }
        }
        else
        {
            List<Node> clientNodes = clientGroupSpec.selectNodes(ensemble, properties);

            nosqlBenchProviders = clientNodes.stream()
                .map(n -> n.getProvider(NoSqlBenchProvider.class))
                .collect(Collectors.toList());
            numClients = nosqlBenchProviders.size();
        }

        // TODO: add whitelist param if contactPoints.size() < serverGroup.size() ?

        Duration histogramFrequency = histogramFrequencySpec.value(properties);

        addOptionIfNotSpecified(NB_VERBOSITY_ARG, "-v");
        addOptionIfNotSpecified(s -> s.equals(NB_STACKTRACES_ARG), NB_STACKTRACES_ARG);

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

            Optional<String> hostParam = hostParamSpec.optionalValue(properties);
            if (hostParam.isPresent())
            {
                clientSpecificArgs.add(hostParam.get().strip());
            }
            else
            {
                List<ServiceContactPointProvider> contactPointProviders = getServiceContactPointProviders(ensemble,
                    properties);
                if (contactPointProviders.isEmpty())
                {
                    String error =
                        String.format("No ServiceContactPointProvider matching the requested service \"%s\" was found",
                            serviceTypeSpec.value(properties));
                    logger().error(error);
                    emitFail("nosqlbench failed: " + error);
                    return;
                }

                List<String> contactPoints = contactPointProviders.stream()
                    .map(ServiceContactPointProvider::getContactPoint)
                    .collect(Collectors.toList());

                clientSpecificArgs.add(String.format("host=%s", String.join(",", contactPoints)));
            }

            workloadDurationSpec.optionalValue(properties).ifPresent(
                duration -> clientSpecificArgs.add(String.format("waitmillis %s stop %s", duration.toMillis(), alias)));
            List<String> orderedCommandParameters =
                Stream.concat(clientSpecificArgs.stream(), options.stream()).collect(Collectors.toList());
            NoSqlBenchProvider nbProvider = nosqlBenchProviders.get(i);
            NodeResponse response = nbProvider
                .nosqlbench(getInstanceName(), clientPrepareScript, orderedCommandParameters, histogramFrequency);
            nosqlBenchCommands.add(response);
        }

        boolean success = Utils.waitForSuccess(logger(), nosqlBenchCommands,
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

    private void addOptionIfNotSpecified(Predicate<String> predicate, String arg)
    {
        if (options.stream().noneMatch(predicate))
        {
            options.add(arg);
        }
    }

    private List<ServiceContactPointProvider> getServiceContactPointProviders(Ensemble ensemble,
        PropertyGroup properties)
    {
        String serviceType = serviceTypeSpec.value(properties);
        List<ServiceContactPointProvider> contactPointProviders;
        if (clientPods)
        {
            contactPointProviders = clientGroup.findAllProviders(ServiceContactPointProvider.class,
                provider -> provider.getServiceName().equals(serviceType))
                .stream()
                .findFirst()
                .stream()
                .collect(Collectors.toList());
        }
        else
        {
            NodeGroup serverGroup = ensemble.getNodeGroupByAlias(serverGroupSpec.getNodeGroupSpec().value(properties));
            List<Node> serverNodes = serverGroupSpec.selectNodes(ensemble, properties);
            // findAllProviders will return all the providers for the given nodegroup. Since serverNodes may be a subset
            // of those nodes, we need to filter out all the providers outside that set.
            contactPointProviders = serverGroup.findAllProviders(ServiceContactPointProvider.class,
                provider -> provider.getServiceName().equals(serviceType) && serverNodes.contains(provider.node()));
        }

        return contactPointProviders;
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
