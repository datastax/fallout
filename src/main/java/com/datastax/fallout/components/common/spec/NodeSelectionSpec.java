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
package com.datastax.fallout.components.common.spec;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;

import com.datastax.driver.core.Host;
import com.datastax.fallout.components.cassandra.CassandraSeedInfoProvider;
import com.datastax.fallout.components.common.provider.ClusterWatcherProvider;
import com.datastax.fallout.harness.ClusterWatcher;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.util.SeededThreadLocalRandom;

/**
 * A {@link NodeSelectionSpec} allows one or more node to be clearly identified by the user.  The user may choose to pick
 * random nodes only from a given rack/dc or from all but a given rack/dc.  This spec contains a node selection policy,
 * a selector for both the dc and the rack, and number of nodes to pick.
 */
public class NodeSelectionSpec
{
    private final PropertySpec<String> nodeGroupSpec;
    private final PropertySpec<NodeSelectionStrategy> nodeSelectionStrategySpec;
    private final PropertySpec<String> selectorSpec;
    private final PropertySpec<Set<Integer>> ordinalsSpec;
    private final PropertySpec<Integer> numberOfNodesSpec;
    private final PropertySpec<Long> forceRandomSeedSpec;
    private final PropertySpec<Boolean> ignoreSeedNodesSpec;

    public NodeSelectionSpec(String prefix)
    {
        this(prefix, "target", false, false);
    }

    public NodeSelectionSpec(String prefix, String prefix2, boolean forceUniquePrefix2, boolean isClient)
    {
        this(prefix, prefix2, forceUniquePrefix2, isClient,
            isClient ? PropertySpecBuilder.clientGroup(prefix) : PropertySpecBuilder.serverGroup(prefix));
    }

    public NodeSelectionSpec(String prefix, String prefix2, boolean forceUniquePrefix2, boolean isClient,
        PropertySpec<String> nodeGroupSpec)
    {
        this(prefix, prefix2, forceUniquePrefix2, isClient, nodeGroupSpec, new NodeSelectionDefaults());
    }

    public NodeSelectionSpec(String prefix, String prefix2, boolean forceUniquePrefix2, boolean isClient,
        PropertySpec<String> nodeGroupSpec, NodeSelectionDefaults defaults)
    {
        this.nodeGroupSpec = nodeGroupSpec;
        nodeSelectionStrategySpec = PropertySpecBuilder
            .createEnum(prefix, NodeSelectionStrategy.class)
            .name(prefix2 + ".strategy")
            .description("How to select the nodes to target")
            .defaultOf(defaults.getNodeSelectionStrategy())
            .build();
        selectorSpec = PropertySpecBuilder
            .createStr(prefix, s -> s.toString().matches(".+:.+"))
            .name(prefix2 + ".selector")
            .description("The selector to use to include/exclude nodes, in the form: <datacenter>:<rack> (ex: " +
                "'*:rack1' for nodes of rack1 in all data centers, 'dc1:*' for nodes of dc1)")
            .defaultOf(defaults.getSelectorSpec())
            .build();
        ordinalsSpec = PropertySpecBuilder.<Set<Integer>>create(prefix)
            .name(prefix2 + ".ordinals")
            .description("The node ordinals to include/exclude (ex: '0,3,6' or 'all'). " +
                "Can not be used together with rack/dc filter.")
            .defaultOf(defaults.getOrdinals())
            .dumper((o) -> o.isEmpty() ? "all" : String.join(",", o.stream()
                .map(Object::toString)
                .collect(Collectors.toSet())))
            .parser(s -> s.equals("all") ? Set.of() : Arrays.stream(s.toString().trim().split(","))
                .map(Integer::valueOf)
                .collect(Collectors.toSet()))
            .build();
        PropertySpecBuilder<Integer> numNodesBuilder = PropertySpecBuilder.createInt(prefix)
            .name(prefix2 + ".number_of_nodes")
            .description("Number of nodes to select (0 means all)")
            .defaultOf(defaults.getNumNodes());
        // backward compatibility hacks
        if (!forceUniquePrefix2)
        {
            numNodesBuilder.deprecatedName("num.nodes");
        }
        if (isClient)
        {
            numNodesBuilder.alias(prefix + "client.size");
        }

        numberOfNodesSpec = numNodesBuilder.build();
        forceRandomSeedSpec = PropertySpecBuilder.createLong(prefix)
            .name(prefix2 + ".seed")
            .description("Force the seed used to initialize the random selection of nodes")
            .defaultOf(defaults.getSeed())
            .build();
        ignoreSeedNodesSpec = PropertySpecBuilder.createBool(prefix, defaults.getIgnoreSeedNodes())
            .name(prefix2 + ".ignore_seed_nodes")
            .description("Will attempt to not select cluster seed nodes. If all your nodes " +
                "are seed nodes, you will never select any nodes.")
            .build();
    }

    public Collection<PropertySpec<?>> getSpecs()
    {
        return List.of(nodeGroupSpec,
            nodeSelectionStrategySpec,
            selectorSpec,
            ordinalsSpec,
            numberOfNodesSpec,
            forceRandomSeedSpec,
            ignoreSeedNodesSpec);
    }

    public PropertySpec<String> getNodeGroupSpec()
    {
        return nodeGroupSpec;
    }

    public NodeSelector createSelector(Ensemble ensemble, PropertyGroup properties)
    {
        return createSelector(ensemble, properties, false);
    }

    public NodeSelector createSelector(Ensemble ensemble, PropertyGroup properties, boolean forceClusterWatcher)
    {
        NodeSelectionTarget target = targetFromProperties(properties);
        NodeGroup nodeGroup = ensemble.getNodeGroupByAlias(target.getNodeGroup());
        return new NodeSelector(ensemble.getControllerGroup().logger(), target, nodeGroup, forceClusterWatcher);
    }

    private NodeSelectionTarget targetFromProperties(PropertyGroup p)
    {
        String nodeGroup = nodeGroupSpec.value(p);
        NodeSelectionStrategy nodeSelectionStrategy = nodeSelectionStrategySpec.value(p);
        String selector = selectorSpec.value(p);
        String datacenter = selector.substring(0, selector.indexOf(':'));
        String rack = selector.substring(selector.indexOf(':') + 1);
        Set<Integer> ordinals = ordinalsSpec.value(p);
        int numberOfNodes = numberOfNodesSpec.value(p);
        long randomSeed = this.forceRandomSeedSpec.value(p);
        boolean ignoreSeedNodes = ignoreSeedNodesSpec.value(p);
        return new NodeSelectionTarget(nodeGroup, nodeSelectionStrategy, ordinals, datacenter, rack, numberOfNodes,
            randomSeed, ignoreSeedNodes);
    }

    public List<Node> selectNodes(Ensemble ensemble, PropertyGroup properties)
    {
        NodeSelector selector = createSelector(ensemble, properties);
        return selector.selectNodes();
    }

    public static class NodeSelectionTarget
    {
        private final NodeSelectionStrategy nodeSelectionStrategy;
        private final String nodeGroup;
        private final Set<Integer> ordinals;
        private final String dc;
        private final String rack;
        private final int numberOfNodes;
        private final SeededThreadLocalRandom random;
        private final boolean ignoreSeedNodes;

        public NodeSelectionTarget(String nodeGroup, NodeSelectionStrategy strategy, Set<Integer> ordinals, String dc,
            String rack, int numNodes,
            long seed)
        {
            this(nodeGroup,
                strategy,
                ordinals,
                dc,
                rack,
                numNodes,
                seed,
                false);
        }

        public NodeSelectionTarget(String nodeGroup, NodeSelectionStrategy strategy, Set<Integer> ordinals, String dc,
            String rack, int numNodes,
            long seed, boolean ignoreSeedNodes)
        {
            this.nodeGroup = nodeGroup;
            this.nodeSelectionStrategy = strategy;
            this.ordinals = ordinals == null ? new HashSet<>() : ordinals;
            this.dc = dc;
            this.rack = rack;
            this.numberOfNodes = numNodes;
            this.random = new SeededThreadLocalRandom(seed);
            this.ignoreSeedNodes = ignoreSeedNodes;
            this.validate();
        }

        private void validate()
        {
            Preconditions.checkArgument(!needsTopologyFilter() || ordinals.isEmpty(),
                "Cant use both ordinal and topology selector");
        }

        public boolean needsTopologyFilter()
        {
            return !dc.equals("*") || !rack.equals("*") || ignoreSeedNodes;
        }

        public boolean keepNode(Host host, Node node)
        {
            // Because node is also a host, we can assume it has a C* Provider.
            boolean canPickSeedOrNodeIsntSeed =
                !ignoreSeedNodes || !node.getProvider(CassandraSeedInfoProvider.class).isSeed();
            if (nodeSelectionStrategy == NodeSelectionStrategy.WHITELIST)
            {
                boolean hasSelectedDc = dc.equals("*") || host.getDatacenter().equals(dc);
                boolean hasSelectedRack = rack.equals("*") || host.getRack().equals(rack);
                return hasSelectedDc && hasSelectedRack && canPickSeedOrNodeIsntSeed;
            }
            else
            {
                boolean hasForbiddenDc = dc.equals("*") || host.getDatacenter().equals(dc);
                boolean hasForbiddenRack = rack.equals("*") || host.getRack().equals(rack);
                return !(hasForbiddenDc && hasForbiddenRack) && canPickSeedOrNodeIsntSeed;
            }
        }

        public String getNodeGroup()
        {
            return nodeGroup;
        }

        public int getNumberOfNodes()
        {
            return numberOfNodes;
        }

        public Random getRandom()
        {
            return random.getRandom();
        }

        public List<Node> pickRandomly(List<Node> nodes, boolean randomOrder)
        {
            int numberOfNodes = getNumberOfNodes();
            if (numberOfNodes <= 0)
            {
                numberOfNodes = nodes.size();
            }
            List<Node> pickedNodes = Utils.pickInRandomOrder(nodes, numberOfNodes, getRandom());
            if (randomOrder)
            {
                return pickedNodes;
            }
            return pickedNodes.stream()
                .sorted(Comparator.comparing(Node::getNodeGroupOrdinal))
                .collect(Collectors.toList());
        }

        public boolean matchesOrdinalFilter(Node n)
        {
            if (ordinals.isEmpty())
            {
                return true;
            }
            boolean contained = ordinals.contains(n.getNodeGroupOrdinal());
            return nodeSelectionStrategy == NodeSelectionStrategy.WHITELIST ? contained : !contained;
        }
    }

    public enum NodeSelectionStrategy
    {
        WHITELIST,
        BLACKLIST
    }

    public static class NodeSelector
    {
        private final NodeSelectionTarget target;
        private final NodeGroup nodeGroup;
        public final Optional<ClusterWatcher> clusterWatcher;

        public NodeSelector(Logger logger, NodeSelectionTarget target, NodeGroup nodeGroup)
        {
            this(logger, target, nodeGroup, false);
        }

        public NodeSelector(Logger logger, NodeSelectionTarget target, NodeGroup nodeGroup,
            boolean forceClusterWatcher)
        {
            this.target = target;
            this.nodeGroup = nodeGroup;
            if (forceClusterWatcher || target.needsTopologyFilter())
            {
                this.clusterWatcher = nodeGroup.findFirstProvider(ClusterWatcherProvider.class)
                    .map(clusterWatcherProvider -> clusterWatcherProvider.createClusterWatcher(logger, nodeGroup));
            }
            else
            {
                this.clusterWatcher = Optional.empty();
            }
        }

        /**
         * @return list of nodes in ordinal order
         */
        public List<Node> selectNodes()
        {
            return selectNodes(false);
        }

        public List<Node> selectNodes(boolean randomOrder)
        {
            List<Node> nodes;
            if (target.needsTopologyFilter())
            {
                if (!clusterWatcher.isPresent())
                {
                    throw new RuntimeException("Cluster Watcher is needed to select nodes, but was not created.");
                }
                nodes = clusterWatcher.get().findMatchingNodes(target, nodeGroup);
            }
            else
            {
                nodes = nodeGroup.getNodes()
                    .stream()
                    .filter(n -> target.matchesOrdinalFilter(n))
                    .collect(Collectors.toList());
            }
            return target.pickRandomly(nodes, randomOrder);
        }
    }

    public boolean selectsASubsetOfNodes(PropertyGroup properties)
    {
        return !(numberOfNodesSpec.value(properties) < 1 &&
            selectorSpec.value(properties).equals("*:*") &&
            ordinalsSpec.value(properties).isEmpty());
    }

    public static class NodeSelectionDefaults
    {
        private NodeSelectionStrategy nodeSelectionStrategy = NodeSelectionStrategy.WHITELIST;
        private String selectorSpec = "*:*";
        private Set<Integer> ordinals = new HashSet<>();
        private int numNodes = 0;
        private long seed = System.nanoTime();
        private boolean ignoreSeedNodes = false;

        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return nodeSelectionStrategy;
        }

        public NodeSelectionDefaults setNodeSelectionStrategy(NodeSelectionStrategy nodeSelectionStrategy)
        {
            this.nodeSelectionStrategy = nodeSelectionStrategy;
            return this;
        }

        public String getSelectorSpec()
        {
            return selectorSpec;
        }

        public NodeSelectionDefaults setSelectorSpec(String selectorSpec)
        {
            this.selectorSpec = selectorSpec;
            return this;
        }

        public Set<Integer> getOrdinals()
        {
            return ordinals;
        }

        public NodeSelectionDefaults setOrdinals(Set<Integer> ordinals)
        {
            this.ordinals = ordinals;
            return this;
        }

        public int getNumNodes()
        {
            return numNodes;
        }

        public NodeSelectionDefaults setNumNodes(int numNodes)
        {
            this.numNodes = numNodes;
            return this;
        }

        public long getSeed()
        {
            return seed;
        }

        public NodeSelectionDefaults setSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public boolean getIgnoreSeedNodes()
        {
            return ignoreSeedNodes;
        }

        public NodeSelectionDefaults setIgnoreSeedNodes(boolean ignoreSeedNodes)
        {
            this.ignoreSeedNodes = ignoreSeedNodes;
            return this;
        }
    }
}
