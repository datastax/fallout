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
package com.datastax.fallout.runner.queue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.harness.TestRunnerTestHelpers;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.runner.ResourceReservationLocks;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;

import static org.assertj.core.api.Assertions.assertThat;

public class ResourceReservationLocksTest extends TestRunnerTestHelpers.QueuingTestRunnerTest
{
    private Logger logger = LoggerFactory.getLogger(ResourceReservationLocksTest.class);

    private Test test;
    private Fakes.TestRunFactory testRunFactory;
    private QueuingTestRunner testRunner;
    private BlockingQueue<UUID> testRunsBeingReserved;
    private BlockingQueue<UUID> testRunsBeingChecked;
    private Semaphore releaseTestRuns;
    private volatile boolean releaseTestRunsTimedOut;
    private ResourceReservationLocks resourceReservationLocks;

    /** Big enough to cope with startup delays, small enough to keep the test short */
    private static final int CLIENT_TIMEOUT_SECONDS = 4;
    private static final int SERVER_TIMEOUT_SECONDS = 2 * CLIENT_TIMEOUT_SECONDS;

    private class ReserveBlockingProvisioner extends FakeProvisioner
    {
        final PropertySpec<UUID> testRunIdSpec = PropertySpecBuilder.create(FakeProvisioner.PREFIX)
            .name("test.run.id")
            .parser(input -> UUID.fromString((String) input))
            .required()
            .build();

        @Override
        public List<PropertySpec> getPropertySpecs()
        {
            return ImmutableList.of(testRunIdSpec);
        }

        @Override
        protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
        {
            if (!Exceptions.getUnchecked(() -> releaseTestRuns.tryAcquire(SERVER_TIMEOUT_SECONDS, TimeUnit.SECONDS)))
            {
                logger.error("Timed out waiting for releaseTestRuns; check that " +
                    "test startup time is < SERVER_TIMEOUT_SECONDS");
                releaseTestRunsTimedOut = true;
                return CheckResourcesResult.FAILED;
            }
            return super.reserveImpl(nodeGroup);
        }
    }

    @Before
    public void setup()
    {
        test = makeTest("fakes.yaml");
        testRunFactory = new Fakes.TestRunFactory();
        testRunsBeingReserved = new LinkedBlockingQueue<>();
        testRunsBeingChecked = new LinkedBlockingQueue<>();
        releaseTestRuns = new Semaphore(0, true);
        releaseTestRunsTimedOut = false;
        resourceReservationLocks = new ResourceReservationLocks();

        testRunner = testRunnerBuilder()
            .withResourceReservationLocks(resourceReservationLocks)
            .withTestRunUpdater(testRun -> {
                if (testRun.getState() == TestRun.State.CHECKING_RESOURCES)
                {
                    testRunsBeingChecked.add(testRun.getTestRunId());
                }
                if (testRun.getState() == TestRun.State.RESERVING_RESOURCES)
                {
                    testRunsBeingReserved.add(testRun.getTestRunId());
                }
                else
                {
                    testRunsBeingReserved.remove(testRun.getTestRunId());
                }
            })
            .withTestRunQueue(new TestRunQueue(new InMemoryPendingQueue(), List::of, Duration.ofMillis(1000),
                resourceReservationLocks::couldAcquire))
            .modifyComponentFactory(componentFactory -> componentFactory
                .mockAll(Provisioner.class, ReserveBlockingProvisioner::new))
            .withGetResourceRequirements(TestRun::getResourceRequirements)
            .build();
    }

    @After
    public void teardown()
    {
        testRunner.close();
        assertThat(releaseTestRunsTimedOut).isFalse();
    }

    private TestRun openstack(String tenant, String instance)
    {
        TestRun testRun = testRunFactory.makeTestRun(test);

        ResourceRequirement requirement = new ResourceRequirement(
            new ResourceRequirement.ResourceType("openstack", tenant, instance), 1);
        requirement.setReservationLockResourceType(new ResourceRequirement.ResourceType("openstack", tenant));

        testRun.setResourceRequirements(ImmutableSet.of(requirement));

        return testRun;
    }

    private UUID[] givenQueuedTestRuns(TestRun... testRuns)
    {
        return Arrays.stream(testRuns)
            .map(testRun -> {
                testRun.setTemplateParamsMap(ImmutableMap.of(
                    "provisionerProperties", "test.run.id: " + testRun.getTestRunId().toString()));
                testRunner.queueTestRun(testRun);
                return testRun.getTestRunId();
            })
            .toArray(UUID[]::new);
    }

    private void assertExactlyTheseTestRunsAreReserved(UUID... testRunIds)
    {
        final List<UUID> reserved = new ArrayList<>(testRunIds.length);
        for (int i = 0; i != testRunIds.length; ++i)
        {
            reserved.add(
                Exceptions.getUnchecked(() -> testRunsBeingReserved.poll(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)));
        }

        assertThat(reserved).containsExactlyInAnyOrder(testRunIds);

        assertThat(Exceptions.getUnchecked(() -> testRunsBeingReserved.poll(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)))
            .isNull();

    }

    private void expectTheseTestRunsToReserveConcurrently(UUID... testRunIds)
    {
        assertExactlyTheseTestRunsAreReserved(testRunIds);
        releaseTestRuns.release(testRunIds.length);
    }

    private void expectTheseTestRunsToReserveSequentially(UUID... testRunIds)
    {
        for (UUID testRunId : testRunIds)
        {
            expectTheseTestRunsToReserveConcurrently(testRunId);
        }
    }

    @org.junit.Test
    public void test_runs_using_non_overlapping_resources_do_not_block_each_other_on_reservation()
    {
        UUID[] testRunIds = givenQueuedTestRuns(
            openstack("performance", "foo"),
            openstack("other", "foo")
        );
        expectTheseTestRunsToReserveConcurrently(
            testRunIds
        );
    }

    @org.junit.Test
    public void test_runs_using_overlapping_resources_block_each_other_on_reservation()
    {
        UUID[] testRunIds = givenQueuedTestRuns(
            openstack("performance", "foo"),
            openstack("performance", "bar")
        );
        expectTheseTestRunsToReserveSequentially(
            testRunIds
        );
    }

    @org.junit.Test
    public void unavailable_test_runs_are_not_processed()
    {
        TestRun blocked = openstack("performance", "foo");
        blocked.setTemplateParamsMap(ImmutableMap.of(
            "provisionerProperties", "test.run.id: " + blocked.getTestRunId().toString()));

        Optional<ResourceReservationLocks.Lock> lockedRequiredResources = resourceReservationLocks.tryAcquire(blocked);

        assertThat(lockedRequiredResources).isPresent();

        testRunner.queueTestRun(blocked);

        assertThat(Exceptions.getUnchecked(() -> testRunsBeingChecked.poll(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)))
            .isNull();

        lockedRequiredResources.get().release();
        releaseTestRuns.release(1);
    }
}
