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
package com.datastax.fallout.ops;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection;

/**
 * Wraps multiple CMs together, the order in which the delegates
 * are operated on will be determined by their dependencies
 */
public class MultiConfigurationManager extends ConfigurationManager
{
    private final ImmutableList<ConfigurationManager> delegates;
    private final List<PropertySpec<?>> allSpecs;

    public MultiConfigurationManager(List<ConfigurationManager> delegates, PropertyGroup nodegroupProperties)
    {
        this(delegates, Set.of(), nodegroupProperties);
    }

    public MultiConfigurationManager(List<ConfigurationManager> delegates,
        Set<Class<? extends Provider>> nodeGroupAvailableProviders, PropertyGroup nodegroupProperties)
    {
        Preconditions.checkArgument(!delegates.isEmpty(), "There must be at least one delegate CM");
        this.delegates =
            determineConfigurationOrder(delegates, nodeGroupAvailableProviders, nodegroupProperties);
        logger().info("Successfully determined MultiConfigurationManager configuration order: {}",
            this.delegates.stream().map(PropertyBasedComponent::name).collect(Collectors.toList()));

        this.allSpecs = delegates.stream().flatMap(cm -> cm.getPropertySpecs().stream()).collect(Collectors.toList());
    }

    @Override
    public void setLogger(Logger logger)
    {
        super.setLogger(logger);
        delegates.stream().forEach(cm -> cm.setLogger(logger));
    }

    @Override
    public void setEnsemble(Ensemble ensemble)
    {
        super.setEnsemble(ensemble);
        delegates.stream().forEach(cm -> cm.setEnsemble(ensemble));
    }

    @Override
    public void setNodeGroup(NodeGroup nodeGroup)
    {
        super.setNodeGroup(nodeGroup);
        getDelegates().forEach(cm -> cm.setNodeGroup(nodeGroup));
    }

    @Override
    public void setFalloutConfiguration(FalloutConfiguration configuration)
    {
        super.setFalloutConfiguration(configuration);
        delegates.stream().forEach(cm -> cm.setFalloutConfiguration(configuration));
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return delegates.stream()
            .flatMap(cm -> cm.getAvailableProviders(nodeGroupProperties).stream())
            .collect(Collectors.toSet());
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        return delegates.stream()
            .flatMap(cm -> cm.getRequiredProviders(nodeGroupProperties).stream())
            .collect(Collectors.toSet());
    }

    private <T> boolean doForTransition(BiFunction<ConfigurationManager, T, Boolean> f, T t,
        TransitionDirection direction)
    {
        Preconditions.checkArgument(direction != TransitionDirection.NONE);
        boolean downwardTransition = direction == TransitionDirection.DOWN;
        Stream<ConfigurationManager> delegatesStream = downwardTransition ?
            Lists.reverse(delegates).stream() :
            delegates.stream();
        return downwardTransition ?
            delegatesStream.map(cm -> f.apply(cm, t)).reduce(true, (acc, cur) -> acc && cur) :
            delegatesStream.allMatch(cm -> f.apply(cm, t)); // fail fast
    }

    @Override
    public void summarizeInfo(InfoConsumer infoConsumer)
    {
        delegates.forEach(d -> d.summarizeInfo(infoConsumer));
    }

    public ImmutableList<ConfigurationManager> getDelegates()
    {
        return delegates;
    }

    private ImmutableList<ConfigurationManager> determineConfigurationOrder(List<ConfigurationManager> delegates,
        Set<Class<? extends Provider>> nodeGroupAvailableProviders, PropertyGroup nodegroupProperties)
    {
        List<ConfigurationManager> unprocessedDelegates = new ArrayList<>(delegates);
        Set<Class<? extends Provider>> availableProviders = new HashSet<>(nodeGroupAvailableProviders);

        ImmutableList.Builder<ConfigurationManager> builder = new ImmutableList.Builder<>();

        while (unprocessedDelegates.size() > 0)
        {
            List<ConfigurationManager> satisfiedDelegates = new ArrayList<>();

            for (ConfigurationManager cm : unprocessedDelegates)
            {
                if (providersAreSatisfied(availableProviders, cm.getRequiredProviders(nodegroupProperties)))
                {
                    satisfiedDelegates.add(cm);
                    availableProviders.addAll(cm.getAvailableProviders(nodegroupProperties));
                }
            }

            builder.addAll(satisfiedDelegates);
            unprocessedDelegates.removeAll(satisfiedDelegates);

            // Check if there are no ConfigurationManagers which are able to configure.
            if (satisfiedDelegates.isEmpty())
            {
                StringBuilder unsatisfiedDependenciesErrorBuilder = new StringBuilder();
                unsatisfiedDependenciesErrorBuilder
                    .append("It is impossible to properly configure this set of Configuration Managers!");
                unprocessedDelegates
                    .forEach(unprocessedDelegate -> {
                        // deterministic order for output (so tests can assert error with a fixed string)
                        List<String> requiredProviders = Utils.toSortedList(
                            unprocessedDelegate.getRequiredProviders(nodegroupProperties),
                            x -> x.getSimpleName());
                        unsatisfiedDependenciesErrorBuilder.append(String.format(
                            " Cannot satisfy dependencies of the %s configuration manager which requires Providers: %s",
                            unprocessedDelegate.name(), requiredProviders));
                    });
                throw new InvalidConfigurationException(unsatisfiedDependenciesErrorBuilder.toString());
            }
        }

        return builder.build();
    }

    private boolean providersAreSatisfied(Set<Class<? extends Provider>> availableProviders,
        Set<Class<? extends Provider>> requiredProviders)
    {
        return requiredProviders.stream().allMatch(
            required -> availableProviders.contains(required) ||
                availableProviders.stream().anyMatch(required::isAssignableFrom)
        );
    }

    @Override
    public boolean configureAndRegisterProviders(NodeGroup nodeGroup)
    {
        return doForTransition(ConfigurationManager::configureAndRegisterProviders, nodeGroup,
            TransitionDirection.UP);
    }

    @Override
    protected boolean configureImpl(NodeGroup nodeGroup)
    {
        return doForTransition(ConfigurationManager::configureImpl, nodeGroup, TransitionDirection.UP);
    }

    @Override
    public boolean registerProviders(Node node)
    {
        return doForTransition(ConfigurationManager::registerProviders, node, TransitionDirection.UP);
    }

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        return doForTransition(ConfigurationManager::startImpl, nodeGroup, TransitionDirection.UP);
    }

    @Override
    protected boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return doForTransition(ConfigurationManager::unconfigureImpl, nodeGroup,
            TransitionDirection.DOWN);
    }

    @Override
    public boolean unregisterProviders(Node node)
    {
        return doForTransition(ConfigurationManager::unregisterProviders, node,
            TransitionDirection.DOWN);
    }

    @Override
    protected boolean stopImpl(NodeGroup nodeGroup)
    {
        return doForTransition(ConfigurationManager::stopImpl, nodeGroup, TransitionDirection.DOWN);
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return logger().withScopedInfo("MultiConfigurationManager.checkState()").get(() -> getDelegates().stream()
            .map(cm -> {
                final NodeGroup.State state = cm.checkStateImpl(nodeGroup);
                logger().info("ConfigurationManager {} checkState returned {}", cm.name(), state);
                return state;
            })
            .filter(NodeGroup.State::isConfigManagementState)
            .min(Comparator.comparing(NodeGroup.State::ordinal))
            .orElse(NodeGroup.State.UNKNOWN));
    }

    @Override
    protected boolean collectArtifactsImpl(Node node)
    {
        return doForTransition(ConfigurationManager::collectArtifactsImpl, node,
            TransitionDirection.DOWN);
    }

    @Override
    protected boolean prepareArtifactsImpl(Node node)
    {
        return doForTransition(ConfigurationManager::prepareArtifactsImpl, node,
            TransitionDirection.DOWN);
    }

    @Override
    public String prefix()
    {
        return "fallout.configuration.management.multi";
    }

    @Override
    public String name()
    {
        return "multi";
    }

    @Override
    public String description()
    {
        return "delegates commands to many CMs";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return allSpecs;
    }

    @Override
    public boolean validatePrefixes(Logger logger)
    {
        return doForTransition(ConfigurationManager::validatePrefixes, logger,
            TransitionDirection.DOWN);
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        delegates.stream().forEach(cm -> cm.validateProperties(properties));
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        delegates.stream().forEach(cm -> cm.validateEnsemble(validator));
    }

    @Override
    public void close()
    {
        delegates.forEach(ConfigurationManager::close);
    }
}
