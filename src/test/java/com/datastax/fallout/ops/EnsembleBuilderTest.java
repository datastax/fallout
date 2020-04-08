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
package com.datastax.fallout.ops;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.configmanagement.NoopConfigurationManager;
import com.datastax.fallout.ops.provisioner.LocalProvisioner;

import static org.assertj.core.api.Assertions.assertThat;

public class EnsembleBuilderTest extends TestHelpers.ArtifactTest
{
    private Ensemble ensemble;

    private NodeGroupBuilder nodeGroupBuilder(String name, int nodeCount)
    {
        return NodeGroupBuilder.create()
            .withNodeCount(nodeCount)
            .withName(name)
            .withConfigurationManager(new NoopConfigurationManager())
            .withProvisioner(new LocalProvisioner())
            .withPropertyGroup(new WritablePropertyGroup());
    }

    @Before
    public void setUp() throws Exception
    {
        ensemble = EnsembleBuilder.create()
            .withControllerGroup(nodeGroupBuilder("controller", 1))
            .withObserverGroup(nodeGroupBuilder("observer", 1))
            .withServerGroup(nodeGroupBuilder("server", 2))
            .withClientGroup(nodeGroupBuilder("client", 2))
            .build(testRunArtifactPath());
    }

    static Pair<String, List<Integer>> nodeGroupOrdinals(NodeGroup nodeGroup)
    {
        return Pair.of(nodeGroup.getName(),
            nodeGroup.getNodes().stream()
                .map(Node::getNodeGroupOrdinal).collect(Collectors.toList()));
    }

    static Pair<String, List<Integer>> nodeGroupOrdinals(String name, Integer... ordinals)
    {
        return Pair.of(name, Arrays.asList(ordinals));
    }

    @Test
    public void nodes_have_node_group_unique_ordinals()
    {
        assertThat(
            ensemble.getUniqueNodeGroupInstances().stream().map(n -> nodeGroupOrdinals(n)))
                .containsExactlyInAnyOrder(
                    nodeGroupOrdinals("controller", 0),
                    nodeGroupOrdinals("observer", 0),
                    nodeGroupOrdinals("client", 0, 1),
                    nodeGroupOrdinals("server", 0, 1)
                );
    }

    @Test
    public void nodes_have_ensemble_unique_ordinals()
    {
        assertThat(
            ensemble.getUniqueNodeGroupInstances().stream()
                .flatMap(nodeGroup -> nodeGroup.getNodes().stream())
                .map(Node::getEnsembleOrdinal))
                    .containsExactlyInAnyOrder(0, 1, 2, 3, 4, 5);
    }
}
