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
package com.datastax.fallout.harness;

import java.util.List;
import java.util.Map;

import com.datastax.fallout.components.common.spec.NodeSelectionSpec;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;

/**
 * The purpose of {@link ClusterWatcher} is to connect to a Cassandra cluster, retrieve its topology through the
 * Datastax java-driver and maintain this connection opened for the entire time the test is run.  The driver will
 * receive the cluster topology change events as they happen and will always be able to provide an accurate view of the
 * cluster, even after nodes are replaced.
 */
public interface ClusterWatcher
{
    /**
     * Returns a list of nodes that match the {@code target} definition from the server nodes of a given {@code
     * ensemble}.  See {@link NodeSelectionSpec} for the supported combinations.
     *
     * @param target   the target definition
     * @param nodeGroup the cluster to pick the nodes from
     *
     * @return a {@link List} of the selected nodes ordered by group ordinal
     */
    List<Node> findMatchingNodes(NodeSelectionSpec.NodeSelectionTarget target, NodeGroup nodeGroup);

    /**
     * Query the cluster metadata and build a mapping of the data centers to their racks, and of the racks to their
     * nodes.
     *
     * @return a {@link Map} associating the data center names to a {@link Map} associating the rack names to the list
     * of nodes they contain
     */
    Map<String, Map<String, List<String>>> currentClusterTopology();
}
