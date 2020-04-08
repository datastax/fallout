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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;

import static org.assertj.core.api.Assertions.assertThat;

public class PrioritizedPendingQueueTest
{
    private Fakes.UUIDFactory uuidFactory = new Fakes.UUIDFactory();
    private Instant createdAt = Instant.parse("2018-09-10T09:00:00Z");
    private Supplier<Instant> nextInstant;

    private List<TestRun> runningTestRuns;
    private PrioritizedPendingQueue queue;

    @Before
    public void setup()
    {
        nextInstant = () -> {
            createdAt = createdAt.plusSeconds(1);
            return createdAt;
        };

        runningTestRuns = new ArrayList<>();
        Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier = () -> List.copyOf(runningTestRuns);
        queue = new PrioritizedPendingQueue(new InMemoryPendingQueue(), runningTestRunsSupplier,
            TestRunQueue.doesNotRequireClusterInUse(runningTestRunsSupplier));
    }

    private void givenTestRunsHaveTheSameCreatedTime()
    {
        nextInstant = () -> createdAt;
    }

    private TestRun createTestRun(String owner)
    {
        TestRun testRun = new TestRun()
        {
            @Override
            public String toString()
            {
                return String.valueOf(getTestRunId().getLeastSignificantBits());
            }
        };
        testRun.setOwner(owner);
        testRun.setTestRunId(uuidFactory.create());
        testRun.setCreatedAt(Date.from(nextInstant.get()));
        return testRun;
    }

    private List<TestRun> givenTestRunsAreQueuedFor(String... owners)
    {
        List<TestRun> testRuns = Arrays.stream(owners).map(this::createTestRun).collect(Collectors.toList());
        testRuns.forEach(queue::add);
        return testRuns;
    }

    private void assertTestRunsAreDequeuedInTheOrder(List<TestRun> testRuns, int... indices)
    {
        List<TestRun> testRunsByPriority = new ArrayList<>();
        while (!queue.noneAvailable())
        {
            queue.remove().ifPresent(testrun -> {
                runningTestRuns.add(testrun);
                testRunsByPriority.add(testrun);
            });
        }

        List<Integer> takenIndicies = new ArrayList<>();
        testRunsByPriority.forEach(testRun -> takenIndicies.add(testRuns.indexOf(testRun)));

        assertThat(takenIndicies.toArray()).isEqualTo(indices);
    }

    @Test
    public void testRuns_are_prioritized_by_number_of_running_testruns_owned_and_creation_time()
    {
        List<TestRun> testRuns = givenTestRunsAreQueuedFor(
            "jenkins",
            "jenkins",
            "tobermory",
            "cholet",
            "bulgaria",
            "orinoco",
            "cholet",
            "jenkins");

        assertTestRunsAreDequeuedInTheOrder(testRuns, 0, 2, 3, 4, 5, 1, 6, 7);
    }

    @Test
    public void testRuns_with_finished_time_are_prioritized_correctly()
    {
        TestRun runningTestRun = createTestRun("jenkins");
        runningTestRuns.add(runningTestRun);
        List<TestRun> testRuns = givenTestRunsAreQueuedFor(
            "tokyo",
            "jenkins",
            "tobermory",
            "cholet",
            "bulgaria",
            "orinoco",
            "cholet",
            "jenkins");

        TestRun testRunToBeRequeued = testRuns.remove(0);
        testRunToBeRequeued.setFinishedAt(Date.from(nextInstant.get()));
        queue.remove(testRunToBeRequeued);
        queue.add(testRunToBeRequeued);

        testRuns.add(testRunToBeRequeued);

        TestRun anotherTestRun = createTestRun("kennedy");
        queue.add(anotherTestRun);
        testRuns.add(anotherTestRun);

        assertTestRunsAreDequeuedInTheOrder(testRuns, 1, 2, 3, 4, 7, 8, 0, 5, 6);
    }

    @Test
    public void testRuns_with_equal_priorities_are_removed_in_the_same_order_as_insertion()
    {
        givenTestRunsHaveTheSameCreatedTime();
        List<TestRun> testRuns = givenTestRunsAreQueuedFor("orinoco", "tobermory");
        assertTestRunsAreDequeuedInTheOrder(testRuns, 0, 1);
    }

    @Test
    public void empty_queue_returns_none_available()
    {
        assertThat(queue.noneAvailable()).isTrue();
    }

    @Test
    public void unavailable_test_runs_are_filtered_out()
    {
        queue = new PrioritizedPendingQueue(new InMemoryPendingQueue(), () -> List.copyOf(runningTestRuns),
            testRun -> !testRun.getOwner().equals("unavailable"));

        List<TestRun> testRuns = givenTestRunsAreQueuedFor("orinoco", "tobermory", "unavailable", "orinoco");
        assertTestRunsAreDequeuedInTheOrder(testRuns, 0, 1, 3);
    }

    @Test
    public void test_runs_which_require_in_use_clusters_are_filtered_out()
    {
        ResourceRequirement resourceRequirement = new ResourceRequirement(
            new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("bravo")), 2);

        TestRun testRun = createTestRun("tokyo");
        testRun.setResourceRequirements(ImmutableSet.of(resourceRequirement));
        runningTestRuns.add(testRun);

        queue.add(testRun);

        assertThat(queue.noneAvailable()).isTrue();
    }

    @Test
    public void test_runs_with_unique_names_can_be_dequeued()
    {
        ResourceRequirement aClusterForReuse = new ResourceRequirement(
            new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("bravo")), 2);

        TestRun testRun = createTestRun("tokyo");
        testRun.setResourceRequirements(ImmutableSet.of(aClusterForReuse));

        queue.add(testRun);

        assertThat(queue.noneAvailable()).isFalse();
    }
}
