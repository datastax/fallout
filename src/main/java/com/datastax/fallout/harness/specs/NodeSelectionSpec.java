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
package com.datastax.fallout.harness.specs;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;

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
    private final PropertySpec<Set<Integer>> ordinalsSpec;
    private final PropertySpec<Integer> numberOfNodesSpec;
    private final PropertySpec<Long> forceRandomSeedSpec;

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
        nodeSelectionStrategySpec = PropertySpecBuilder.create(prefix)
            .name(prefix2 + ".strategy")
            .description("How to select the nodes to target")
            .optionsArray(NodeSelectionStrategy.values())
            .defaultOf(defaults.getNodeSelectionStrategy())
            .parser(s -> NodeSelectionStrategy.valueOf(s.toString().toUpperCase()))
            .build();
        ordinalsSpec = PropertySpecBuilder.<Set<Integer>>create(prefix)
            .name(prefix2 + ".ordinals")
            .description("The node ordinals to include/exclude (ex: '0,3,6' or 'all'). " +
                "Can not be used together with rack/dc filter.")
            .defaultOf(defaults.getOrdinals())
            .dumper((o) -> o.isEmpty() ? "all" : String.join(",", o.stream()
                .map(Object::toString)
                .collect(Collectors.toSet())))
            .validator(s -> s instanceof Set || s.toString().trim().matches("^all$|^\\d+(\\s*,\\s*\\d+)*$"))
            .parser(s -> s.equals("all") ? Collections.emptySet() : Arrays.stream(s.toString().trim().split(","))
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
    }

    public Collection<PropertySpec<?>> getSpecs()
    {
        return Arrays.asList(nodeGroupSpec,
            nodeSelectionStrategySpec,
            ordinalsSpec,
            forceRandomSeedSpec);
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
        String serverGroup = nodeGroupSpec.value(p);
        NodeSelectionStrategy nodeSelectionStrategy = nodeSelectionStrategySpec.value(p);
        Set<Integer> ordinals = ordinalsSpec.value(p);
        int numberOfNodes = numberOfNodesSpec.value(p);
        long randomSeed = this.forceRandomSeedSpec.value(p);
        return new NodeSelectionTarget(serverGroup, nodeSelectionStrategy, ordinals, numberOfNodes, randomSeed);
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
        private final int numberOfNodes;
        private final SeededThreadLocalRandom random;

        public NodeSelectionTarget(String nodeGroup, NodeSelectionStrategy strategy, Set<Integer> ordinals,
            int numNodes, long seed)
        {
            this.nodeGroup = nodeGroup;
            this.nodeSelectionStrategy = strategy;
            this.ordinals = ordinals == null ? new HashSet<>() : ordinals;
            this.numberOfNodes = numNodes;
            this.random = new SeededThreadLocalRandom(seed);
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
            Collections.sort(pickedNodes, Comparator.comparing(Node::getNodeGroupOrdinal));
            return pickedNodes;
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
        private final NodeGroup serverGroup;

        public NodeSelector(Logger logger, NodeSelectionTarget target, NodeGroup serverGroup)
        {
            this(logger, target, serverGroup, false);
        }

        public NodeSelector(Logger logger, NodeSelectionTarget target, NodeGroup serverGroup,
            boolean forceClusterWatcher)
        {
            this.target = target;
            this.serverGroup = serverGroup;
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
            List<Node> nodes = serverGroup.getNodes()
                .stream()
                .filter(n -> target.matchesOrdinalFilter(n))
                .collect(Collectors.toList());
            return target.pickRandomly(nodes, randomOrder);
        }
    }

    public static class NodeSelectionDefaults
    {
        private NodeSelectionStrategy nodeSelectionStrategy = NodeSelectionStrategy.WHITELIST;
        private HashSet<Integer> ordinals = new HashSet<>();
        private int numNodes = 0;
        private long seed = System.nanoTime();

        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return nodeSelectionStrategy;
        }

        public NodeSelectionDefaults setNodeSelectionStrategy(NodeSelectionStrategy nodeSelectionStrategy)
        {
            this.nodeSelectionStrategy = nodeSelectionStrategy;
            return this;
        }

        public HashSet<Integer> getOrdinals()
        {
            return ordinals;
        }

        public NodeSelectionDefaults setOrdinals(HashSet<Integer> ordinals)
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
    }
}
