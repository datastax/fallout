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
package com.datastax.fallout.harness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.FalloutPropertySpecs;
import com.datastax.fallout.ops.JobConsoleLoggers;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.ops.configmanagement.NoopConfigurationManager;
import com.datastax.fallout.ops.provisioner.LocalProvisioner;
import com.datastax.fallout.runner.UserCredentialsFactory;
import com.datastax.fallout.service.core.TestRun;

import static org.assertj.core.api.Assertions.assertThat;

public class ActiveTestRunTransitionTest extends TestHelpers.FalloutTest
{
    private static final String NODE_GROUP_NAME = "atr-transition-test-server";
    private static final NodeGroup.State DEFAULT_LAUNCH_RUNLEVEL = NodeGroup.State.STARTED_SERVICES_RUNNING;
    private static final NodeGroup.State DEFAULT_FINAL_RUNLEVEL = NodeGroup.State.DESTROYED;

    private ActiveTestRun withRunLevels(Optional<NodeGroup.State> launchState, Optional<NodeGroup.State> finalState)
    {
        NodeGroupBuilder testGroupBuilder = nodeGroupBuilder(launchState);
        finalState.ifPresent(ignored -> testGroupBuilder.withFinalRunLevel(finalState));
        return withRunLevels(testGroupBuilder);
    }

    private ActiveTestRun withRunLevels(NodeGroupBuilder testGroupBuilder)
    {
        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withServerGroup(testGroupBuilder)
            .withClientGroup(testGroupBuilder);

        return ActiveTestRunBuilder.create()
            .withTestRunName("active-test-run-transition-test")
            .withEnsembleBuilder(ensembleBuilder, true)
            .withWorkload(new Workload(new ArrayList<>(), new HashMap<>(), new HashMap<>()))
            .withTestRunStatusUpdater(
                new TestRunAbortedStatusUpdater(new InMemoryTestRunStateStorage(TestRun.State.CREATED)))
            .withLoggers(new JobConsoleLoggers())
            .withResourceChecker((e) -> ImmutableList.of(CompletableFuture.completedFuture(true)))
            .withTestRunArtifactPath(persistentTestOutputDir())
            .withUserCredentials(new UserCredentialsFactory.UserCredentials(getTestUser()))
            .build();
    }

    private NodeGroupBuilder nodeGroupBuilder(Optional<NodeGroup.State> launchState)
    {
        WritablePropertyGroup pg = new WritablePropertyGroup();
        launchState.ifPresent(ls -> pg.put(FalloutPropertySpecs.launchRunLevelPropertySpec.name(), ls));

        return NodeGroupBuilder.create()
            .withProvisioner(new LocalProvisioner())
            .withConfigurationManager(new NoopConfigurationManager())
            .withPropertyGroup(pg)
            .withNodeCount(1)
            .withName(NODE_GROUP_NAME);
    }

    private void testActiveTestRunTransitions(ActiveTestRun atr, NodeGroup.State launchLevel,
        NodeGroup.State finalLevel)
    {
        NodeGroup testGroup = atr.getEnsemble().getNodeGroupByAlias(NODE_GROUP_NAME);
        assertThat(testGroup.getState()).isEqualTo(NodeGroup.State.UNKNOWN);

        atr.setup();
        assertThat(testGroup.getState()).isEqualTo(launchLevel);

        atr.tearDown();
        assertThat(testGroup.getState()).isEqualTo(finalLevel);
    }

    @Test
    public void an_active_test_run_given_no_runlevels_will_transition_to_the_default_launch_and_final_runlevel()
    {
        testActiveTestRunTransitions(
            withRunLevels(Optional.empty(), Optional.empty()),
            DEFAULT_LAUNCH_RUNLEVEL, DEFAULT_FINAL_RUNLEVEL);
    }

    @Test
    public void an_active_test_run_given_an_launch_runlevel_will_start_the_workload_at_that_runlevel()
    {
        NodeGroup.State launchLevel = NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        testActiveTestRunTransitions(
            withRunLevels(Optional.of(launchLevel), Optional.empty()),
            launchLevel, DEFAULT_FINAL_RUNLEVEL);
    }

    @Test
    public void an_active_test_run_given_a_final_runlevel_will_be_torn_down_to_that_runlevel()
    {
        NodeGroup.State finalLevel = NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        testActiveTestRunTransitions(
            withRunLevels(Optional.empty(), Optional.of(finalLevel)),
            DEFAULT_LAUNCH_RUNLEVEL, finalLevel);
    }

    @Test
    public void an_active_test_run_given_both_launch_and_final_runlevels_will_start_and_end_at_those_runlevels()
    {
        NodeGroup.State launchLevel = NodeGroup.State.STARTED_SERVICES_CONFIGURED;
        NodeGroup.State finalLevel = NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        testActiveTestRunTransitions(
            withRunLevels(Optional.of(launchLevel), Optional.of(finalLevel)),
            launchLevel, finalLevel);
    }

    @Test
    public void an_active_test_run_marked_for_reuse_will_preserve_the_nodegroup_state()
    {
        NodeGroupBuilder testGroupBuilder = nodeGroupBuilder(Optional.empty());
        testGroupBuilder.withFinalRunLevel(Optional.empty()); // mark_for_reuse: true in yaml
        testActiveTestRunTransitions(
            withRunLevels(testGroupBuilder), DEFAULT_LAUNCH_RUNLEVEL, DEFAULT_LAUNCH_RUNLEVEL);
    }
}
