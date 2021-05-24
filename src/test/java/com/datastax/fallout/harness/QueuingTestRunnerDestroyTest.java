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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class QueuingTestRunnerDestroyTest extends TestRunnerTestHelpers.QueuingTestRunnerTest<FalloutConfiguration>
{
    private void destroysNodeGroupTest(
        Function<AtomicInteger, Provisioner> provisionerFactory)
    {
        final AtomicInteger nodeGroupDestroyed = new AtomicInteger(0);
        final CompletableFuture<TestRun> completedTestRun = new CompletableFuture<>();

        try (QueuingTestRunner testRunner = testRunnerBuilder()
            .modifyComponentFactory(componentFactory -> componentFactory
                .mockAll(Provisioner.class, () -> provisionerFactory.apply(nodeGroupDestroyed)))
            .withTestRunCompletionCallback(completedTestRun::complete)
            .build())
        {
            final Test test = makeTest("fakes.yaml");
            final TestRun testRun = testRunFactory.makeTestRun(test);
            testRunner.queueTestRun(testRun);

            assertThat(completedTestRun.join())
                .isEqualTo(testRun)
                .hasState(TestRun.State.FAILED)
                .hasFailedDuring(TestRun.State.SETTING_UP);

            assertThat(nodeGroupDestroyed.get()).isEqualTo(1);
        }
    }

    @org.junit.jupiter.api.Test
    public void destroys_the_nodegroup_when_prepare_fails()
    {
        destroysNodeGroupTest(nodeGroupDestroyed -> new FakeProvisioner() {
            private NodeGroup.State checkStateResult = NodeGroup.State.DESTROYED;

            @Override
            protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
            {
                return checkStateResult;
            }

            @Override
            protected boolean prepareImpl(NodeGroup nodeGroup)
            {
                checkStateResult = NodeGroup.State.CREATED;
                return false;
            }

            @Override
            protected boolean destroyImpl(NodeGroup nodeGroup)
            {
                nodeGroupDestroyed.incrementAndGet();
                return true;
            }
        });
    }

    @org.junit.jupiter.api.Test
    public void destroys_the_nodegroup_when_prepare_fails_and_checkState_is_broken()
    {
        destroysNodeGroupTest(nodeGroupDestroyed -> new FakeProvisioner() {
            @Override
            protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
            {
                return nodeGroup.getState() == NodeGroup.State.FAILED ? nodeGroup.getState() :
                    super.checkStateImpl(nodeGroup);
            }

            @Override
            protected boolean prepareImpl(NodeGroup nodeGroup)
            {
                return false;
            }

            @Override
            protected boolean destroyImpl(NodeGroup nodeGroup)
            {
                nodeGroupDestroyed.incrementAndGet();
                return true;
            }
        });
    }
}
