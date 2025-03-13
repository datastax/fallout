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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
    private static final long SHUTDOWN_GRACE_MILLIS = 300000; // 5 minutes

    private static final PropertySpec<Integer> numClientsSpecs = PropertySpecBuilder.createInt(prefix)
        .name("num_clients")
        .description("Number of nosqlbench clients to execute. Default is all replicas.")
        .build();

    protected static final NodeSelectionSpec serverGroupSpec = new NodeSelectionSpec(prefix, "server", true, false);
    private static final NodeSelectionSpec clientGroupSpec = new NodeSelectionSpec(prefix, "client", true, true);

    protected static final PropertySpec<String> serviceTypeSpec = PropertySpecBuilder.createStr(prefix)
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

    private PropertySpec<Boolean> equivalentCyclesSpec = PropertySpecBuilder.createBool(prefix, false)
        .name("equivalent_cycles")
        .description(
            "When true, give each client the same cycle range. The entire cycle range will be given to each client")
        .build();

    private static final PropertySpec<List<String>> argsListSpec = PropertySpecBuilder.createStrList(prefix)
        .name("args")
        .description("List of arguments to pass into nosqlbench command.")
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
            "Host argument in its entirety, e.g. host=10.0.0.1 or secureconnectbundle=path/to/bundle.zip Note: environment variables are not available. Optional when running named scenario.")
        .build();

    private static final PropertySpec<String> scenarioParamSpec = PropertySpecBuilder.createStr(prefix)
        .name("scenario")
        .description(
            "Name or URL of a scenario you wish to run. Note: This cannot be set with duration or alias. See: http://docs.nosqlbench.io/#/docs/designing_workloads/10_named_scenarios.md ")
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
        clientsRunInKubernetes = validator.nodeGroupWillHaveProvider(clientGroupSpec, NoSqlBenchPodProvider.class);
        if (clientsRunInKubernetes)
        {
            if (!validateKubernetesClients(validator))
            {
                return;
            }
        }
        else
        {
            validator.nodeGroupRequiresProvider(clientGroupSpec, NoSqlBenchProvider.class);
        }

        boolean targetsServerGroup = hostParamSpec.optionalValue(getProperties()).isEmpty();
        if (targetsServerGroup)
        {
            validateServerGroup(validator);
        }
    }

    protected boolean validateKubernetesClients(EnsembleValidator validator)
    {
        // client pods and server pods have to run on the same k8s cluster nodegroup
        Optional<NodeGroup> clientGroup = validator.getNodeGroup(clientGroupSpec);
        Optional<NodeGroup> serverGroup = validator.getNodeGroup(serverGroupSpec);
        if (clientGroup.isEmpty() || serverGroup.isEmpty() || !clientGroup.get().equals(serverGroup.get()))
        {
            validator.addValidationError("client and server groups must be the same nodegroup");
        }
        validator.nodeGroupRequiresProvider(clientGroupSpec, NoSqlBenchPodProvider.class);
        return true;
    }

    protected void validateServerGroup(EnsembleValidator validator)
    {
        validator.nodeGroupRequiresProvider(serverGroupSpec, ServiceContactPointProvider.class);
    }

    @Override
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(numClientsSpecs, totalCyclesSpec, cycleOffsetSpec, equivalentCyclesSpec, argsListSpec,
                histogramFrequencySpec, syncStartSpec, workloadDurationSpec, aliasSpec, hostParamSpec, serviceTypeSpec,
                scenarioParamSpec)
            .addAll(serverGroupSpec.getSpecs())
            .addAll(clientGroupSpec.getSpecs())
            .addAll(timeoutSpec.getSpecs())
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        if (scenarioParamSpec.optionalValue(properties).isPresent() &&
            (aliasSpec.optionalValue(properties).isPresent() ||
                workloadDurationSpec.optionalValue(properties).isPresent()))
        {
            throw new PropertySpec.ValidationException(
                String.format("Cannot set %s at the same time as either of (%s, %s)",
                    scenarioParamSpec.name(), aliasSpec.name(), workloadDurationSpec.name()));
        }
        if (scenarioParamSpec.optionalValue(properties).isEmpty() && argsListSpec.optionalValue(properties).isEmpty())
        {
            throw new PropertySpec.ValidationException(
                String.format("%s property spec is required when not running a named scenario", argsListSpec.name()));
        }
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
    private NodeGroup serverGroup;
    private NodeGroup clientGroup;
    private boolean clientsRunInKubernetes;

    protected NodeGroup getServerGroup()
    {
        return serverGroup;
    }

    protected NodeGroup getClientGroup()
    {
        return clientGroup;
    }

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        serverGroup = ensemble.getNodeGroupByAlias(serverGroupSpec.getNodeGroupSpec().value(properties));
        clientGroup = ensemble.getNodeGroupByAlias(clientGroupSpec.getNodeGroupSpec().value(properties));
        Optional<FileProvider.RemoteFileProvider> remoteFileProvider = clientGroup.findFirstProvider(
            FileProvider.RemoteFileProvider.class);

        scenarioParamSpec.optionalValue(properties).ifPresentOrElse(scenario -> args.add(scenario), () -> {
            args.add(workloadDurationSpec.optionalValue(properties).isPresent() ? "start" : "run");
            alias = aliasSpec.optionalValue(properties).orElseGet(this::getInstanceName);
            args.add(String.format("alias=%s", alias));
        });

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
        replacePlaceHoldersInArgs();
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        List<NoSqlBenchProvider> nosqlBenchProviders = null;
        List<String> clientPrepareScripts = null;
        int numClients = -1;
        if (clientsRunInKubernetes)
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
                clientPrepareScripts = Collections.nCopies(
                    numClients,
                    String.format("curl -sf http://countdownlatch.org/%d/%s && ", numClients, syncId)
                );
            }
            else
            {
                clientPrepareScripts = Collections.nCopies(numClients, "");
            }
        }
        else
        {
            List<Node> clientNodes = clientGroupSpec.selectNodes(ensemble, properties);

            nosqlBenchProviders = clientNodes.stream()
                .map(n -> n.getProvider(NoSqlBenchProvider.class))
                .toList();
            numClients = nosqlBenchProviders.size();
            clientPrepareScripts = new ArrayList<>();
            for (int i = 0; i < numClients; i++)
            {
                String nbStateDir = clientNodes.get(i).getRemoteLibraryPath();
                clientPrepareScripts.add(String.format("export NBSTATEDIR=%s && ", nbStateDir));
            }
        }

        // TODO: add whitelist param if contactPoints.size() < serverGroup.size() ?

        Duration histogramFrequency = histogramFrequencySpec.value(properties);

        addOptionIfNotSpecified(NB_VERBOSITY_ARG, "-v");
        addOptionIfNotSpecified(s -> s.equals(NB_STACKTRACES_ARG), NB_STACKTRACES_ARG);

        Long totalCycles = totalCyclesSpec.value(properties);
        Long cycleOffset = cycleOffsetSpec.value(properties);
        boolean equivalentCycles = equivalentCyclesSpec.value(properties);

        Optional<List<String>> cycleRanges = Optional.empty();
        if (totalCycles != null)
        {
            cycleRanges = Optional.of(
                buildCycleRangeByClientArgs(totalCycles, cycleOffset, numClients, equivalentCycles));
        }

        emitInvoke("Starting nosqlbench");
        List<NodeResponse> nosqlBenchCommands = new ArrayList<>();
        for (int i = 0; i < numClients; i++)
        {
            Node clientNode = nosqlBenchProviders.get(i).node();
            List<String> clientSpecificArgs = new ArrayList<>(args);

            boolean graphiteExplicitlyConfigured =
                argsListContains("--report-graphite-to");

            if (!graphiteExplicitlyConfigured)
            {
                clientSpecificArgs.addAll(discoverClientGraphiteArgs(clientNode));
            }

            boolean promPushExplicitlyConfigured =
                argsListContains("--report-prompush-to");

            if (!promPushExplicitlyConfigured)
            {
                boolean setPromPushArgs = false;
                String nbVersionStr = nosqlBenchProviders.get(i).fetchVersionInfo();
                // assume user knows what they're doing in this case (child class will check for PromPushProvider)
                if (nbVersionStr.equals("unknown"))
                {
                    setPromPushArgs = true;
                }
                else
                {
                    NoSqlBenchProvider.Version nbVersion = new NoSqlBenchProvider.Version(nbVersionStr);
                    if (nbVersion.isGTE(5, 21))
                        setPromPushArgs = true;
                }
                if (setPromPushArgs)
                {
                    if (!maybeSetupPromPushApiKey(ensemble, clientNode))
                    {
                        logger().error("Unable to properly set Prometheus PushGateway API key");
                    }
                    clientSpecificArgs.addAll(discoverClientPromPushArgs(clientNode));
                }
            }

            if (cycleRanges.isPresent())
            {
                clientSpecificArgs.add(String.format("cycles=%s", cycleRanges.get().get(i)));
            }

            // determine appropriate server connection configs
            clientSpecificArgs.addAll(getClientConnectionArgs(clientNode, ensemble, properties));

            if (workloadDurationSpec.optionalValue(properties).isPresent())
            {
                Duration duration = workloadDurationSpec.value(properties);
                String nbVersionStr = nosqlBenchProviders.get(i).fetchVersionInfo();
                if (!nbVersionStr.equals("unknown"))
                {
                    NoSqlBenchProvider.Version nbVersion = new NoSqlBenchProvider.Version(nbVersionStr);
                    if (nbVersion.isGTE(5, 21))
                        clientSpecificArgs
                            .add(String.format("wait ms=%s stop activity=%s", duration.toMillis(), alias));
                    else
                        clientSpecificArgs.add(String.format("waitmillis %s stop %s", duration.toMillis(), alias));
                }
                else
                {
                    clientSpecificArgs.add(String.format("waitmillis %s stop %s", duration.toMillis(), alias));
                }
            }

            List<String> orderedCommandParameters =
                Stream.concat(clientSpecificArgs.stream(), options.stream()).toList();
            NoSqlBenchProvider nbProvider = nosqlBenchProviders.get(i);
            String clientPrepareScript = clientPrepareScripts.get(i);
            NodeResponse response = nbProvider
                .nosqlbench(getInstanceName(), clientPrepareScript, orderedCommandParameters, histogramFrequency);
            nosqlBenchCommands.add(response);
        }

        boolean success = Utils.waitForSuccess(logger(), nosqlBenchCommands,
            wo -> wo.timeout = workloadTimeout(properties));
        if (success)
        {
            emitOk("nosqlbench successful");
        }
        else
        {
            emitFail("nosqlbench failed");
        }
    }

    protected List<String> discoverClientGraphiteArgs(Node clientNode)
    {
        return List.of();
    }

    protected List<String> discoverClientPromPushArgs(Node clientNode)
    {
        return List.of();
    }

    protected boolean maybeSetupPromPushApiKey(Ensemble ensemble, Node clientNode)
    {
        return true;
    }

    private List<String> getClientConnectionArgs(Node clientNode, Ensemble ensemble, PropertyGroup properties)
    {
        Optional<String> hostParam = hostParamSpec.optionalValue(properties);

        // When scenario is set, host is optional and must be explicit to be set at all.
        // Otherwise host param takes precedence over service contact points.
        if (hostParam.isPresent())
        {
            return List.of(hostParam.get().strip());
        }

        if (scenarioParamSpec.optionalValue(properties).isPresent())
        {
            // Do nothing, allow the scenario to define the host parameters
            return List.of();
        }

        // e.g. if using the generic HTTP driver, we leave connection details to the user
        if (serviceTypeSpec.value(properties).equals("other"))
        {
            return List.of();
        }

        List<String> dsClientConnectionArgs = getDsClientConnectionArgs(clientNode, ensemble, properties);
        if (!dsClientConnectionArgs.isEmpty())
        {
            return dsClientConnectionArgs;
        }

        List<String> contactPoints = getServiceContactPointProviders(ensemble, properties).stream()
            .map(ServiceContactPointProvider::getContactPoint)
            .toList();
        return List.of(String.format("host=%s", String.join(",", contactPoints)));
    }

    protected List<String> getArgs()
    {
        return args;
    }

    protected void setArgs(List<String> args)
    {
        this.args = args;
    }

    protected List<String> getOptions()
    {
        return options;
    }

    protected void setOptions(List<String> options)
    {
        this.options = options;
    }

    private boolean argsListContains(String subString)
    {
        return Stream.concat(options.stream(), args.stream())
            .anyMatch(s -> s.contains(subString));
    }

    protected void replacePlaceHoldersInArgs()
    {
    }

    protected List<String> getDsClientConnectionArgs(Node client, Ensemble ensemble, PropertyGroup properties)
    {
        return List.of();
    }

    private Duration workloadTimeout(PropertyGroup properties)
    {
        return workloadDurationSpec.optionalValue(properties)
            .map(workloadDuration -> new Duration(workloadDuration.toMillis() + SHUTDOWN_GRACE_MILLIS,
                TimeUnit.MILLISECONDS))
            .orElseGet(() -> timeoutSpec.toDuration(properties));
    }

    private void addOptionIfNotSpecified(Predicate<String> predicate, String arg)
    {
        if (options.stream().noneMatch(predicate))
        {
            options.add(arg);
        }
    }

    protected List<ServiceContactPointProvider> getServiceContactPointProviders(Ensemble ensemble,
        PropertyGroup properties)
    {
        String serviceType = serviceTypeSpec.value(properties);
        List<ServiceContactPointProvider> contactPointProviders;
        if (clientsRunInKubernetes)
        {
            contactPointProviders = serverGroup.findAllProviders(ServiceContactPointProvider.class,
                provider -> provider.getServiceName().equals(serviceType))
                .stream()
                .findFirst()
                .stream()
                .toList();
        }
        else
        {
            contactPointProviders = serverGroupSpec.selectNodes(ensemble, properties)
                .stream()
                .flatMap(n -> n.maybeGetAllProviders(ServiceContactPointProvider.class))
                .filter(p -> serviceType.equals(p.getServiceName()))
                .toList();
        }
        if (contactPointProviders.isEmpty())
        {
            throw new RuntimeException(String.format(
                "No ServiceContactPointProvider matching the requested service \"%s\" was found",
                serviceType));
        }
        return contactPointProviders;
    }

    public static List<String> buildCycleRangeByClientArgs(long totalCycles, long cycleOffset, int numClients,
        boolean equivalentCycles)
    {
        if (equivalentCycles)
        {
            long cycleEnd = totalCycles + cycleOffset;
            return new ArrayList<>(Collections.nCopies(numClients, String.format("%d..%d", cycleOffset, cycleEnd)));
        }
        return distributeCycleRangeByClient(totalCycles, cycleOffset, numClients)
            .map(r -> String.format("%d..%d", r.lowerEndpoint(), r.upperEndpoint()))
            .toList();
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
