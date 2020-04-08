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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;

import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;

import static org.assertj.core.api.Assertions.assertThat;

public class QueuingTestRunnerResourceTest extends TestRunnerTestHelpers.QueuingTestRunnerTest
{
    @org.junit.Test
    public void queueTestRun_updates_status() throws InterruptedException
    {
        class BlockingCheckResourcesProvisioner extends FakeProvisioner
        {
            private final CountDownLatch startedLatch;
            private final CountDownLatch finishedLatch;

            private BlockingCheckResourcesProvisioner(CountDownLatch startedLatch, CountDownLatch finishedLatch)
            {
                this.startedLatch = startedLatch;
                this.finishedLatch = finishedLatch;
            }

            @Override
            public Optional<ResourceRequirement> getResourceRequirements(NodeGroup nodeGroup)
            {
                startedLatch.countDown();
                try
                {
                    finishedLatch.await();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
                return Optional.empty();
            }
        }

        CountDownLatch startedLatch = new CountDownLatch(1);
        CountDownLatch finishedLatch = new CountDownLatch(1);

        final Test test = makeTest("fakes.yaml");
        final Map<UUID, TestRun.State> testRunStatus = Collections.synchronizedMap(new HashMap<>());

        final TestRun testRunA = testRunFactory.makeTestRun(test);
        final TestRun testRunB = testRunFactory.makeTestRun(test);

        final Consumer<TestRun> testRunUpdater =
            (testRun) -> testRunStatus.put(testRun.getTestRunId(), testRun.getState());

        try (QueuingTestRunner testRunner = testRunnerBuilder()
            .withTestRunUpdater(testRunUpdater)
            .modifyComponentFactory(componentFactory -> componentFactory.mockAll(Provisioner.class,
                () -> new BlockingCheckResourcesProvisioner(startedLatch, finishedLatch)))
            .modifyActiveTestRunFactory((ignored) -> {
            })
            .withTestRunCompletionCallback((ignored1) -> {
            })
            .build())
        {
            testRunner.queueTestRun(testRunA);
            testRunner.queueTestRun(testRunB);

            startedLatch.await();
            try
            {
                assertThat(testRunStatus)
                    .isEqualTo(ImmutableMap.of(
                        testRunA.getTestRunId(), TestRun.State.CHECKING_RESOURCES,
                        testRunB.getTestRunId(), TestRun.State.CREATED));
            }
            finally
            {
                finishedLatch.countDown();
            }
        }
    }
}
