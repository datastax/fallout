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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.configmanagement.FakeConfigurationManager;
import com.datastax.fallout.ops.providers.FakeProvider;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

public class ClusterReuseTest extends EnsembleFalloutTest
{
    @Test
    public void reusing_a_cluster_that_depends_on_a_provider_registered_in_a_non_reused_cluster_works()
    {
        performTestRunWithMockedComponents("reused_depends_on_non_reused.yaml",
            mockedComponentFactory -> mockedComponentFactory
                .mockNamed(Provisioner.class, "reused", () -> new FakeProvisioner()
                {
                    @Override
                    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                    {
                        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
                    }
                })

                .mockNamed(ConfigurationManager.class, "reused", () -> new FakeConfigurationManager()
                {
                    @Override
                    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                    {
                        return NodeGroup.State.STARTED_SERVICES_RUNNING;
                    }

                    @Override
                    public boolean registerProviders(Node node)
                    {
                        ensemble.getObserverGroup().getNodes().get(0).waitForProvider(FakeProvider.class);
                        return true;
                    }
                })

                .mockNamed(Provisioner.class, "not-reused", () -> new FakeProvisioner())

                .mockNamed(ConfigurationManager.class, "not-reused", () -> new FakeConfigurationManager()
                {
                    @Override
                    public boolean registerProviders(Node node)
                    {
                        new FakeProvider(node);
                        return true;
                    }
                }));
    }

    @Test
    public void post_check_state_actions_finish_before_attempting_to_transition_the_reused_cluster()
    {
        var startImplCalled = new CompletableFuture<Void>();
        var continueRegisterProviders = new CompletableFuture<Void>();

        var performTestRun = CompletableFuture
            .supplyAsync(() -> performTestRunWithMockedComponents("providers_registered_before_transition.yaml",
                mockingComponentFactory -> mockingComponentFactory
                    .mockNamed(Provisioner.class, "reused", () -> new FakeProvisioner()
                    {
                        @Override
                        protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                        {
                            return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
                        }
                    })

                    .mockNamed(ConfigurationManager.class, "reused", () -> new FakeConfigurationManager()
                    {
                        @Override
                        protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                        {
                            return NodeGroup.State.STARTED_SERVICES_CONFIGURED;
                        }

                        @Override
                        protected boolean startImpl(NodeGroup nodeGroup)
                        {
                            startImplCalled.complete(null);
                            return true;
                        }

                        @Override
                        public boolean registerProviders(Node node)
                        {
                            continueRegisterProviders.join();
                            return true;
                        }
                    })));

        assertThatThrownBy(() -> startImplCalled.get(5, TimeUnit.SECONDS))
            .isInstanceOf(TimeoutException.class);

        continueRegisterProviders.complete(null);

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(startImplCalled).isCompleted());

        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(performTestRun).isCompleted());
    }
}
