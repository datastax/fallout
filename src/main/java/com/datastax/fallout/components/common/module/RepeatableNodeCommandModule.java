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
package com.datastax.fallout.components.common.module;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.common.spec.NodeSelectionSpec;
import com.datastax.fallout.components.common.spec.RepeatableActionWithDelay;
import com.datastax.fallout.components.common.spec.TimeoutSpec;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.ops.PropertySpecBuilder.createStr;

/**
 * Basic module to run repeatable commands.
 */
public abstract class RepeatableNodeCommandModule<CP extends Provider> extends Module
{
    private final Class<CP> commandProviderClass;

    private final PropertySpec<String> commandSpec;
    private final PropertySpec<String> commandSecondarySpec;
    private final NodeSelectionSpec nodesSpec;
    private final RepeatableActionWithDelay iterationSpec;
    private final TimeoutSpec timeoutSpec;

    private final PropertySpec<Duration> noOutputTimeoutSpec;

    private Ensemble ensemble;
    private String command;
    private String secondaryCommand;
    private NodeSelectionSpec.NodeSelector nodeSelector;
    private List<Node> targetNodes = new ArrayList<>();
    private Duration timeoutDuration;
    private Optional<Duration> noOutputTimeout;

    public RepeatableNodeCommandModule(Class<CP> commandProviderClass, String prefix)
    {
        this(commandProviderClass, prefix, NodeSelectionSpec::new);
    }

    public RepeatableNodeCommandModule(Class<CP> commandProviderClass, String prefix,
        Function<String, NodeSelectionSpec> nodeSelectionSpecGenerator)
    {
        super(RunToEndOfPhaseMethod.MANUAL, Lifetime.RUN_TO_END_OF_PHASE);
        this.commandProviderClass = commandProviderClass;

        commandSpec = createStr(prefix)
            .name("command")
            .description(commandDescription())
            .required()
            .build();

        commandSecondarySpec = createStr(prefix)
            .name("command.secondary")
            .description("run a alternate command every other run i.e. disablegossip/enablegossip")
            .build();

        iterationSpec = new RepeatableActionWithDelay(prefix);
        nodesSpec = nodeSelectionSpecGenerator.apply(prefix);
        timeoutSpec = new TimeoutSpec(prefix);

        noOutputTimeoutSpec = PropertySpecBuilder.createDuration(prefix)
            .name("no_output_timeout")
            .defaultOf(NodeResponse.WaitOptions.DEFAULT_NO_OUTPUT_TIMEOUT)
            .description(
                "Length of time this command is allowed to remain silent before it's timed out. A value <= 0 results in no timeout.")
            .build();
    }

    @Override
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(commandSpec, commandSecondarySpec)
            .addAll(nodesSpec.getSpecs())
            .addAll(iterationSpec.getSpecs())
            .addAll(timeoutSpec.getSpecs())
            .add(noOutputTimeoutSpec)
            .build();
    }

    public String getCommand(PropertyGroup properties)
    {
        return commandSpec.value(properties);
    }

    protected NodeSelectionSpec getNodesSpec()
    {
        return nodesSpec;
    }

    protected List<Node> getTargetNodes()
    {
        return targetNodes;
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        validator.nodeGroupRequiresProvider(getNodesSpec(), commandProviderClass);
    }

    protected Class<CP> getCommandProviderClass()
    {
        return commandProviderClass;
    }

    private void validateCommandHelper(String command, PropertySpec<String> commandSpec)
    {
        try
        {
            validateCommand(command);
        }
        catch (Throwable t)
        {
            throw new PropertySpec.ValidationException(commandSpec,
                "Command '" + command + "' failed validation:\n" + t);
        }
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        String command = getCommand(properties);
        validateCommandHelper(command, commandSpec);

        commandSecondarySpec.optionalValue(properties)
            .ifPresent(secondaryCommand -> validateCommandHelper(secondaryCommand, commandSecondarySpec));
    }

    /**
     * subclasses can raise an exception to validate the command at yaml submission time
     */
    protected void validateCommand(String command)
    {
    }

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        this.ensemble = ensemble;
        command = getCommand(properties);
        secondaryCommand = commandSecondarySpec.value(properties);
        this.iterationSpec.init(getUnfinishedRunOnceModules(), properties);

        nodeSelector = nodesSpec.createSelector(ensemble, properties);
        timeoutDuration = timeoutSpec.toDuration(properties);
        Duration noOutputTimeout = noOutputTimeoutSpec.value(properties);
        this.noOutputTimeout = noOutputTimeout.value > 0L ? Optional.of(noOutputTimeout) : Optional.empty();

        logger().info(name() + " setup done.");
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        logger().info("Starting " + name() + " module");
        try
        {
            while (!isTestRunAborted() && iterationSpec.shouldExecute())
            {
                final String commandToRun;
                if (iterationSpec.getCurrentIteration() % 2 == 1 && secondaryCommand != null)
                {
                    commandToRun = secondaryCommand;
                }
                else
                {
                    targetNodes = nodeSelector.selectNodes();
                    commandToRun = command;
                }
                iterationSpec.executeDelayed(() -> runCommand(commandToRun));
            }

            if (runsToEndOfPhase())
            {
                logger().info("No more commands to perform, awaiting concurrent modules");
                getUnfinishedRunOnceModules().await();
            }
            else
            {
                logger().info("No more commands to perform");
            }
        }
        catch (InterruptedException e)
        {
            logger().warn("Interrupted while running " + name() + " module");
            // Restore interruption status
            Thread.currentThread().interrupt();
        }
    }

    protected List<CP> getProviders()
    {
        List<CP> providers = targetNodes.stream()
            .map(n -> n.maybeGetProvider(commandProviderClass))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

        if (providers.size() != targetNodes.size())
        {
            throw new RuntimeException(
                "Not all selected nodes have required command provider of class: " + commandProviderClass);
        }

        return providers;
    }

    private void runCommand(String command)
    {
        List<NodeResponse> responses = new ArrayList<>(targetNodes.size());
        List<CP> providers = getProviders();

        if (providers.isEmpty())
        {
            throw new RuntimeException("No providers found for class: " + commandProviderClass);
        }
        for (CP provider : providers)
        {
            String node = provider.node().getId();

            emitInvoke(String.format("Executing command %s on %s", command, node));
            responses.add(runCommand(provider, command));
        }

        if (Utils.waitForSuccess(logger(), responses, wo -> {
            wo.timeout = timeoutDuration;
            wo.noOutputTimeout = noOutputTimeout;
        }))
        {
            String nodeIds = targetNodes.stream()
                .map(Node::getId)
                .collect(Collectors.joining(", "));

            if (evaluateResult(responses))
            {
                String msg = String.format("%s executed command '%s' on %s successfully", name(), command, nodeIds);
                emitOk(msg);
            }
            else
            {
                String msg = String.format("%s executed command '%s' on %s with errors.", name(), command, nodeIds);
                logger().error(msg);
                emitFail(msg);
            }
        }
        else
        {
            String msg = String.format("%s executed command '%s' and timed out or errored.", name(), command);
            logger().error(msg);
            emitFail(msg);
        }
    }

    protected abstract String commandDescription();

    protected abstract NodeResponse runCommand(CP provider, String command);

    /**
     * allows subclasses to do more elaborate success checking.
     * we already know all responses exited with code 0.
     */
    protected boolean evaluateResult(Collection<NodeResponse> responses)
    {
        return true;
    }

    @Override
    public void teardown(Ensemble ensemble, PropertyGroup properties)
    {
        String secondaryCommand = commandSecondarySpec.value(properties);
        // Put back in the initial state
        if (secondaryCommand != null)
        {
            runCommand(secondaryCommand);
        }
        super.teardown(ensemble, properties);
    }
}
