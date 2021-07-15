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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import com.datastax.fallout.harness.TestRunLinkUpdater;
import com.datastax.fallout.harness.WorkloadComponent;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory.LocalScratchSpace;
import com.datastax.fallout.runner.CheckResourcesResult;

/**
 * Represents the top-level Ops API of a Fallout test.
 *
 * Different actions are performed on different nodes based on their Role.
 *
 * The easiest way to create an Ensemble is to use the Builder.
 *
 * @see EnsembleBuilder
 */
public class Ensemble implements DebugInfoProvidingComponent, AutoCloseable
{
    public enum Role
    {
        SERVER, //Represents a service like Cassandra
        CLIENT, //Represents a service user like cassandra-stress
        OBSERVER, //Represents an observer like OpsCenter/Graphite
        CONTROLLER //Represents an external controller like Jepsen
    }

    final UUID testRunId;
    final List<NodeGroup> serverGroups;
    final List<NodeGroup> clientGroups;
    final NodeGroup observerGroup;
    final NodeGroup controllerGroup;
    private final TestRunLinkUpdater testRunLinkUpdater;
    private final LocalScratchSpace workloadScratchSpace;
    private final Logger logger;
    private final LocalFilesHandler localFilesHandler;

    protected Ensemble(UUID testRunId, List<NodeGroup> serverGroups, List<NodeGroup> clientGroups,
        NodeGroup observerGroup, NodeGroup controllerGroup, TestRunLinkUpdater testRunLinkUpdater,
        LocalScratchSpace workloadScratchSpace, Logger logger,
        LocalFilesHandler localFilesHandler)
    {
        this.testRunId = testRunId;
        this.serverGroups = serverGroups;
        this.clientGroups = clientGroups;
        this.controllerGroup = controllerGroup;
        this.observerGroup = observerGroup;
        this.testRunLinkUpdater = testRunLinkUpdater;
        this.workloadScratchSpace = workloadScratchSpace;
        this.logger = logger;
        this.localFilesHandler = localFilesHandler;

        for (NodeGroup nodeGroup : getUniqueNodeGroupInstances())
        {
            ConfigurationManager cm = nodeGroup.getConfigurationManager();
            cm.setEnsemble(this);
        }
    }

    public Logger logger()
    {
        return logger;
    }

    @Override
    public void close()
    {
        getAllNodeGroups().forEach(NodeGroup::close);
    }

    public UUID getTestRunId()
    {
        return this.testRunId;
    }

    public List<NodeGroup> getAllNodeGroups()
    {
        List<NodeGroup> res = new ArrayList<>();
        for (NodeGroup serverGroup : serverGroups)
        {
            res.add(serverGroup);
        }
        for (NodeGroup clientGroup : clientGroups)
        {
            res.add(clientGroup);
        }
        res.add(controllerGroup);
        res.add(observerGroup);
        return res;
    }

    public NodeGroup getNodeGroupByAlias(String name)
    {
        return getNodeGroup(getAllNodeGroups(), name, name);
    }

    public Optional<NodeGroup> maybeGetNodeGroupByAlias(String name)
    {
        return maybeGetNodeGroup(getAllNodeGroups(), name, name);
    }

    public NodeGroup getServerGroup(PropertySpec<String> serverGroupSpec, PropertyGroup properties)
    {
        return getServerGroup(serverGroupSpec.value(properties));
    }

    public NodeGroup getServerGroup(String name)
    {
        return getNodeGroup(serverGroups, name, "server");
    }

    public List<NodeGroup> getServerGroups()
    {
        return serverGroups;
    }

    public NodeGroup getClientGroup(PropertySpec<String> clientGroupSpec, PropertyGroup properties)
    {
        return getClientGroup(clientGroupSpec.value(properties));
    }

    public NodeGroup getClientGroup(String name)
    {
        return getNodeGroup(clientGroups, name, "client");
    }

    public List<NodeGroup> getClientGroups()
    {
        return clientGroups;
    }

    private static NodeGroup getNodeGroup(List<NodeGroup> nodeGroups, String name, String alias)
    {
        Optional<NodeGroup> nodeGroup = maybeGetNodeGroup(nodeGroups, name, alias);
        return nodeGroup.orElseThrow(() -> {
            throw new IllegalStateException(
                "Unknown NodeGroup: " + name + " - available: " + nodeGroups.stream()
                    .map(c -> c.name)
                    .sorted()
                    .collect(Collectors.toList()));
        });
    }

    private static Optional<NodeGroup> maybeGetNodeGroup(List<NodeGroup> nodeGroups, String name, String alias)
    {
        if ((name == null || alias.equalsIgnoreCase(name)) && nodeGroups.size() == 1)
        {
            return Optional.of(nodeGroups.get(0));
        }
        for (NodeGroup nodeGroup : nodeGroups)
        {
            for (String ngAlias : nodeGroup.aliases)
            {
                if (ngAlias.equals(name))
                {
                    return Optional.of(nodeGroup);
                }
            }
        }
        return Optional.empty();
    }

    public NodeGroup getObserverGroup()
    {
        return observerGroup;
    }

    public NodeGroup getControllerGroup()
    {
        return controllerGroup;
    }

    public NodeGroup getGroup(Role role, String name)
    {
        switch (role)
        {
            case SERVER:
                return getServerGroup(name);
            case CLIENT:
                return getClientGroup(name);
            case CONTROLLER:
                return controllerGroup;
            case OBSERVER:
                return observerGroup;
            default:
                throw new IllegalStateException("Unknown group role: " + role);
        }
    }

    public LocalScratchSpace makeScratchSpaceFor(WorkloadComponent component)
    {
        return workloadScratchSpace.makeScratchSpaceFor(component);
    }

    /** Transition entire ensemble at once */
    public CompletableFuture<Boolean> transitionState(NodeGroup.State state)
    {
        return transitionState(state, NodeGroup::transitionState);
    }

    private CompletableFuture<Boolean> transitionState(
        NodeGroup.State state,
        BiFunction<NodeGroup, NodeGroup.State, CompletableFuture<CheckResourcesResult>> transitionNodeGroup)
    {
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();

        for (NodeGroup nodeGroup : getUniqueNodeGroupInstances())
        {
            futures.add(transitionNodeGroup.apply(nodeGroup, state)
                .thenApplyAsync(checkResourcesResult -> checkResourcesResult != CheckResourcesResult.FAILED));
        }

        return Utils.waitForAllAsync(futures);
    }

    /**
     * Last-ditch option to really destroy an ensemble
     * @return
     */
    public CompletableFuture<Boolean> forceDestroy()
    {
        try
        {
            // Only force tear down node groups not marked for reuse
            List<CompletableFuture<Boolean>> futures = getUniqueNodeGroupInstances().stream()
                .filter(ng -> !ng.isMarkedForReuse())
                .map(NodeGroup::forceDestroy)
                .collect(Collectors.toList());
            return Utils.waitForAllAsync(futures);
        }
        catch (Throwable e)
        {
            logger.error("FAILURE ON forceDestroy(): PLEASE CHECK STATUS OF ANY NODES IN ENSEMBLE ", e);
            return CompletableFuture.completedFuture(false);
        }
    }

    public Set<NodeGroup> getUniqueNodeGroupInstances()
    {
        Map<NodeGroup, Boolean> uniques = new IdentityHashMap<>();
        for (NodeGroup nodeGroup : getAllNodeGroups())
        {
            uniques.put(nodeGroup, true);
        }
        return uniques.keySet();
    }

    /**
     * Downloads artifacts from each nodeGroup in the ensemble to the given localPath
     * @return
     */
    public CompletableFuture<Boolean> downloadArtifacts()
    {
        List<CompletableFuture<Boolean>> futures = getUniqueNodeGroupInstances().stream()
            .map(group -> group.downloadArtifacts())
            .collect(Collectors.toList());

        return Utils.waitForAllAsync(futures, logger, "downloading artifacts from ensemble");
    }

    public <P extends Provider> Optional<P> findFirstProvider(Class<P> providerClass)
    {
        return findFirstNodeWithProvider(providerClass).map(n -> n.getProvider(providerClass));
    }

    public Optional<Node> findFirstNodeWithProvider(Class<? extends Provider> providerClass)
    {
        return getUniqueNodeGroupInstances().stream()
            .flatMap(ng -> ng.getNodes().stream())
            .filter(n -> n.hasProvider(providerClass))
            .findFirst();
    }

    public <P extends Provider> Optional<P> findProviderWithName(Class<P> providerClass, String name)
    {
        return getUniqueNodeGroupInstances().stream()
            .flatMap(ng -> ng.getNodes().stream())
            .filter(n -> n.hasProvider(providerClass) && n.getProvider(providerClass).name().equals(name))
            .map(n -> n.getProvider(providerClass))
            .findFirst();
    }

    private <T extends Provider> Supplier<IllegalArgumentException> missingProviderError(Class<T> providerClass)
    {
        return () -> new IllegalArgumentException("Ensemble is missing a Node with a " + providerClass.getSimpleName());
    }

    public <T extends Provider> T findFirstRequiredProvider(Class<T> providerClass)
    {
        return findFirstProvider(providerClass).orElseThrow(missingProviderError(providerClass));
    }

    public CompletableFuture<Boolean> prepareArtifacts()
    {
        return actOnAllUniqueNodeGroups(NodeGroup::prepareArtifacts, "preparing artifacts from ensemble");
    }

    public CompletableFuture<Boolean> collectArtifacts()
    {
        return actOnAllUniqueNodeGroups(NodeGroup::collectArtifacts, "collecting artifacts from ensemble");
    }

    private CompletableFuture<Boolean> actOnAllUniqueNodeGroups(Function<NodeGroup, CompletableFuture<Boolean>> action,
        String operation)
    {
        List<CompletableFuture<Boolean>> futures = getUniqueNodeGroupInstances()
            .stream()
            .map(action)
            .collect(Collectors.toList());
        return Utils.waitForAllAsync(futures, logger, operation);
    }

    public Set<ResourceRequirement> getResourceRequirements()
    {
        return ResourceRequirement.reducedResourceRequirements(getUniqueNodeGroupInstances().stream()
            .map(NodeGroup::getResourceRequirements)
            .filter(Optional::isPresent)
            .map(Optional::get));
    }

    public void summarizeInfo(InfoConsumer infoConsumer)
    {
        for (NodeGroup ng : getAllNodeGroups())
        {
            ng.summarizeInfo(infoConsumer);
        }
    }

    public void addLink(String linkName, String link)
    {
        testRunLinkUpdater.add(linkName, link);
    }

    public boolean createAllLocalFiles()
    {
        return localFilesHandler.createAllLocalFiles(this);
    }

    @Override
    public String toString()
    {
        return "Ensemble{" +
            "serverGroups=" + serverGroups +
            ", clientGroups=" + clientGroups +
            ", observerGroup=" + observerGroup +
            ", controllerGroup=" + controllerGroup +
            '}';
    }
}
