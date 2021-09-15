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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.FalloutPropertySpecs;
import com.datastax.fallout.ops.JobConsoleLoggers;
import com.datastax.fallout.ops.JobLoggers;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.runner.UserCredentialsFactory;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class ActiveTestRunTransitionTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    private static final String NODE_GROUP_NAME = "atr-transition-test-server";
    private static final NodeGroup.State DEFAULT_LAUNCH_RUNLEVEL = NodeGroup.State.STARTED_SERVICES_RUNNING;
    private static final NodeGroup.State DEFAULT_FINAL_RUNLEVEL = NodeGroup.State.DESTROYED;

    private ActiveTestRun withRunLevels(Optional<NodeGroup.State> launchState, Optional<NodeGroup.State> finalState)
    {
        NodeGroupBuilder testGroupBuilder = nodeGroupBuilderWithLaunchState(launchState);
        finalState.ifPresent(ignored -> testGroupBuilder.withFinalRunLevel(finalState));
        return createActiveTestRun(testGroupBuilder);
    }

    private ActiveTestRun createActiveTestRunWithLoggers(NodeGroupBuilder testGroupBuilder,
        Function<UserCredentialsFactory.UserCredentials, JobLoggers> jobLoggersFactory)
    {
        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withServerGroup(testGroupBuilder)
            .withClientGroup(testGroupBuilder);

        UserCredentialsFactory.UserCredentials userCredentials =
            new UserCredentialsFactory.UserCredentials(getTestUser(), Optional.empty());

        final var loggers = jobLoggersFactory.apply(userCredentials);

        return ActiveTestRunBuilder.create()
            .withTestRunIdentifier(Fakes.TEST_RUN_IDENTIFIER)
            .withEnsembleBuilder(ensembleBuilder)
            .withWorkload(new Workload(new ArrayList<>(), new HashMap<>(), new HashMap<>()))
            .withTestRunStatusUpdater(
                new TestRunAbortedStatusUpdater(new InMemoryTestRunStateStorage(TestRun.State.CREATED)))
            .withLoggers(loggers)
            .withResourceChecker((e) -> List.of(CompletableFuture.completedFuture(true)))
            .withTestRunArtifactPath(persistentTestOutputDir())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .withFalloutConfiguration(falloutConfiguration())
            .withUserCredentials(userCredentials)
            .build();
    }

    private ActiveTestRun createActiveTestRun(NodeGroupBuilder testGroupBuilder)
    {
        return createActiveTestRunWithLoggers(testGroupBuilder, JobConsoleLoggers::new);
    }

    private NodeGroupBuilder nodeGroupBuilder(Optional<NodeGroup.State> launchState, CheckResourcesResult createResult)
    {
        WritablePropertyGroup pg = new WritablePropertyGroup();
        launchState.ifPresent(ls -> pg.put(FalloutPropertySpecs.launchRunLevelPropertySpec.name(), ls));

        return NodeGroupBuilder.create()
            .withProvisioner(new FakeProvisioner() {
                @Override
                protected CheckResourcesResult createImpl(NodeGroup nodeGroup)
                {
                    return createResult;
                }
            })
            .withConfigurationManager(new FakeConfigurationManager())
            .withPropertyGroup(pg)
            .withNodeCount(1)
            .withName(NODE_GROUP_NAME);
    }

    private NodeGroupBuilder nodeGroupBuilderWithLaunchState(Optional<NodeGroup.State> launchState)
    {
        return nodeGroupBuilder(launchState, CheckResourcesResult.AVAILABLE);
    }

    private NodeGroupBuilder nodeGroupBuilderWithCreateResult(CheckResourcesResult createResult)
    {
        return nodeGroupBuilder(Optional.empty(), createResult);
    }

    private void testActiveTestRunTransitions(ActiveTestRun atr, NodeGroup.State launchLevel,
        NodeGroup.State finalLevel)
    {
        NodeGroup testGroup = atr.getEnsemble().getNodeGroupByAlias(NODE_GROUP_NAME);
        assertThat(testGroup.getState()).isEqualTo(NodeGroup.State.UNKNOWN);

        atr.setup();
        assertThat(testGroup.getState()).isEqualTo(launchLevel);

        atr.tearDownEnsemble();
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
        NodeGroupBuilder testGroupBuilder = nodeGroupBuilderWithLaunchState(Optional.empty());
        testGroupBuilder.withFinalRunLevel(Optional.empty()); // mark_for_reuse: true in yaml
        testActiveTestRunTransitions(
            createActiveTestRun(testGroupBuilder), DEFAULT_LAUNCH_RUNLEVEL, DEFAULT_LAUNCH_RUNLEVEL);
    }

    private static class ErrorSensingJobConsoleLoggers extends JobConsoleLoggers
    {
        private final AtomicBoolean errorLogged;

        public ErrorSensingJobConsoleLoggers(UserCredentialsFactory.UserCredentials userCredentials,
            AtomicBoolean errorLogged)
        {
            super(userCredentials);
            this.errorLogged = errorLogged;
        }

        @Override
        public Logger create(String name, Path ignored)
        {
            final var logger = (ch.qos.logback.classic.Logger) super.create(name, ignored);

            final var loggerContext = logger.getLoggerContext();

            final var appender = new AppenderBase<ILoggingEvent>() {
                @Override
                protected void append(ILoggingEvent event)
                {
                    if (event.getLevel() == Level.ERROR)
                    {
                        errorLogged.set(true);
                    }
                }
            };
            appender.setContext(loggerContext);
            appender.start();

            logger.addAppender(appender);
            return logger;
        }
    }

    @Test
    public void unavailable_resources_during_setup_will_not_log_an_error()
    {
        final var errorLogged = new AtomicBoolean(false);
        final var activeTestRun = createActiveTestRunWithLoggers(
            nodeGroupBuilderWithCreateResult(CheckResourcesResult.UNAVAILABLE),
            userCredentials -> new ErrorSensingJobConsoleLoggers(userCredentials, errorLogged));

        assertThat(activeTestRun.setup()).isEqualTo(CheckResourcesResult.UNAVAILABLE);
        assertThat(errorLogged).isFalse();
    }

    @Test
    public void failure_during_setup_will_log_an_error()
    {
        final var errorLogged = new AtomicBoolean(false);
        final var activeTestRun = createActiveTestRunWithLoggers(
            nodeGroupBuilderWithCreateResult(CheckResourcesResult.FAILED),
            userCredentials -> new ErrorSensingJobConsoleLoggers(userCredentials, errorLogged));

        assertThat(activeTestRun.setup()).isEqualTo(CheckResourcesResult.FAILED);
        assertThat(errorLogged).isTrue();
    }
}
