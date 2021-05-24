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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvider;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class ClusterReuseTest extends EnsembleFalloutTest<FalloutConfiguration>
{
    private final static Logger logger = LoggerFactory.getLogger(ClusterReuseTest.class);

    @Test
    public void reusing_a_cluster_that_depends_on_a_provider_registered_in_a_non_reused_cluster_works()
    {
        performTestRunWithMockedComponents("reused_depends_on_non_reused.yaml",
            mockedComponentFactory -> mockedComponentFactory
                .mockNamed(Provisioner.class, "reused", () -> new FakeProvisioner() {
                    @Override
                    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                    {
                        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
                    }
                })

                .mockNamed(ConfigurationManager.class, "reused", () -> new FakeConfigurationManager() {
                    @Override
                    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                    {
                        return NodeGroup.State.STARTED_SERVICES_RUNNING;
                    }

                    @Override
                    public boolean registerProviders(Node node)
                    {
                        getEnsemble().getObserverGroup().getNodes().get(0).waitForProvider(FakeProvider.class);
                        return true;
                    }
                })

                .mockNamed(Provisioner.class, "not-reused", () -> new FakeProvisioner())

                .mockNamed(ConfigurationManager.class, "not-reused", () -> new FakeConfigurationManager() {
                    @Override
                    public boolean registerProviders(Node node)
                    {
                        new FakeProvider(node);
                        return true;
                    }
                }));
    }

    // Run multiple times in an attempt to provoke a race
    @RepeatedTest(10)
    public void post_check_state_actions_finish_before_attempting_to_transition_the_reused_cluster()
    {
        final var nodeGroupCalls = new LinkedBlockingQueue<String>();

        performTestRunWithMockedComponents("providers_registered_before_transition.yaml",
            mockingComponentFactory -> mockingComponentFactory
                .mockNamed(Provisioner.class, "reused", () -> new FakeProvisioner() {
                    @Override
                    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                    {
                        logger.debug("P.checkState({})", getNodeGroup().getName());
                        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
                    }
                })

                .mockNamed(ConfigurationManager.class, "reused", () -> new FakeConfigurationManager() {
                    @Override
                    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                    {
                        logger().debug("CM.checkState({})", getNodeGroup().getName());
                        return NodeGroup.State.STARTED_SERVICES_CONFIGURED;
                    }

                    @Override
                    protected boolean startImpl(NodeGroup nodeGroup)
                    {
                        nodeGroupCalls.add("startImpl");
                        return true;
                    }

                    @Override
                    public boolean registerProviders(Node node)
                    {
                        return logger().withScopedDebug("registerProviders({})", node.getNodeGroup().getName())
                            .get(() -> {
                                if (node.getNodeGroup().getName().equals("server"))
                                {
                                    // Make the server call delay for a short period: if we're not waiting for
                                    // all post-check-state actions to complete, then this _should_ make some
                                    // of the startImpl calls appear before registerProviders completes.
                                    Exceptions.runUnchecked(() -> Thread.sleep(100));
                                }
                                nodeGroupCalls.add("registerProviders");
                                return true;
                            });
                    }
                }));

        assertThat(nodeGroupCalls)
            .as("All the registerProviders calls made as part of post check-state " +
                "actions must complete before startImpl is called")
            .containsExactly("registerProviders", "registerProviders", "startImpl", "startImpl");
    }

    @Test
    public void failure_during_setup_does_not_cause_an_assertion_during_teardown()
    {
        final var markFailedWithReasonCalls = new AtomicInteger();

        final var testDefinition = getTestClassResource("mark_for_reuse.yaml");
        final var activeTestRun = createActiveTestRunBuilder()
            .withTestDefinitionFromYaml(testDefinition)
            .withTestRunStatusUpdater(
                new TestRunAbortedStatusUpdater(new InMemoryTestRunStateStorage(TestRun.State.CREATED)) {
                    @Override
                    public synchronized void markFailedWithReason(TestRun.State finalState)
                    {
                        markFailedWithReasonCalls.incrementAndGet();
                        super.markFailedWithReason(finalState);
                    }
                })
            .withComponentFactory(new TestRunnerTestHelpers.MockingComponentFactory()
                .mockAll(Provisioner.class, () -> new FakeProvisioner() {
                    @Override
                    protected boolean prepareImpl(NodeGroup nodeGroup)
                    {
                        return false;
                    }
                }))
            .build();

        activeTestRun.run(logger::error);

        // There should be only two errors: one because of the failed prepare on the
        // nodegroup, and one for overall setup failures.
        assertThat(markFailedWithReasonCalls).hasValue(2);
    }
}
