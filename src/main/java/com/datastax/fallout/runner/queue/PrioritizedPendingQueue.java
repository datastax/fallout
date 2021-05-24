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
package com.datastax.fallout.runner.queue;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ScopedLogger;

/**
 * Wrapper class for {@link PendingQueue} which prioritizes the queue by highest to lowest priority according to the
 * following rules:
 *
 * <ol>
 * <li> Test runs who fail the "available" predicate, or that require a specific cluster already in use, will be
 * filtered out.
 * <li> Test runs with an owner that already has tests in runningTestRuns will be given a lower priority than
 * test runs with an owner that has no tests in runningTestRuns.  The more tests owned in runningTestRuns, the lower the
 * priority.
 * <li> Then by how long they have been in the queue.
 * </ol>
 */
public class PrioritizedPendingQueue
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(PrioritizedPendingQueue.class);

    private final PendingQueue pendingQueue;
    private final Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier;
    private final Predicate<ReadOnlyTestRun> available;

    @VisibleForTesting
    public PrioritizedPendingQueue(PendingQueue pendingQueue, Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier,
        Predicate<ReadOnlyTestRun> available)
    {
        this.pendingQueue = pendingQueue;
        this.runningTestRunsSupplier = runningTestRunsSupplier;
        this.available = testRun -> logger.withScopedDebug("available({})", testRun.getShortName())
            .get(() -> available.test(testRun));
    }

    public void add(TestRun testRun)
    {
        pendingQueue.add(testRun);
    }

    public boolean remove(TestRun testRun)
    {
        return pendingQueue.remove(testRun);
    }

    private Optional<TestRun> firstAvailable()
    {
        return pending().stream()
            .filter(available)
            .findFirst();
    }

    public Optional<TestRun> remove()
    {
        Optional<TestRun> testRun = firstAvailable();
        testRun.ifPresent(pendingQueue::remove);
        return testRun;
    }

    public List<TestRun> pending()
    {
        return pendingQueue.pending().stream()
            .sorted(comparator(runningTestRunsSupplier.get()))
            .collect(Collectors.toList());
    }

    public boolean noneAvailable()
    {
        return firstAvailable().isEmpty();
    }

    /**
     * Returns a comparator that will order {@link TestRun}s from highest to lowest priority according to the following
     * rules:
     *
     * <ol>
     *
     *     <li> Testruns with an owner that already has tests in runningTestRuns will
     *     be given a lower priority than testruns with an owner that has no tests in
     *     runningTestRuns.  The more tests owned in runningTestRuns, the lower the priority;
     *
     *     <li> Time-since-last-check: the finished time; if null, the created time.  The older, the higher.
     *
     * </ol>
     *
     * Why time-since-last-check instead of time-on-queue?  If we use time-on-queue, and given:
     *
     * <ul>
     *     <li>we have a long queue of testruns all waiting for the same resource (A), and:
     *     <li>the <code>available</code> predicate uses {@link com.datastax.fallout.runner.ResourceReservationLocks} to prevent two testruns from trying to reserve the same resource simultaneously;
     * </ul>
     *
     * then:
     *
     * <ul>
     *
     *     <li> the oldest testrun will try and reserve resources, locking them
     *     using {@link com.datastax.fallout.runner.ResourceReservationLocks};
     *
     *     <li> all other testruns using the same resources will be considered
     *     unavailable because the resources (A) they need are locked for reservation;
     *
     *     <li> when the oldest testrun fails to get resources (not necessarily the same resources as
     *     the other waiting testruns: it could be waiting on both A and some other scarcer resource
     *     B), it will set its finished time, and the resource reservation lock will be released;
     *
     *     <li> because we prioritise the <em>oldest</em> testrun, the same testrun will be
     *     selected again, and none of the other testruns requiring A will get a chance to run
     *     until the oldest testrun acquires resources i.e. the other testruns will be starved.
     *
     * </ul>
     *
     * Using time-since-last-check means that we will work our way through _all_ the waiting testruns in rotation.
     * <p>
     * <b>NB:</b> this strategy only prevents starvation via {@link
     * com.datastax.fallout.runner.ResourceReservationLocks}; it won't stop other forms.  In particular, a large queue
     * of testruns that need a small number of resource A can starve a testrun that needs a large number of resource
     * A: if only enough resources for a small testrun become available at any one time, the small testruns will always
     * get those resources, and they won't become freed up in sufficient quantity for the large testrun to start.
     */
    private static Comparator<TestRun> comparator(List<ReadOnlyTestRun> runningTestRuns)
    {
        final Map<String, Long> reverseOwnerPriority = runningTestRuns.stream()
            .map(ReadOnlyTestRun::getOwner)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        return Comparator
            .<TestRun>comparingLong(testRun -> reverseOwnerPriority.getOrDefault(testRun.getOwner(), 0L))
            .thenComparing(
                testRun -> testRun.getFinishedAt() != null ? testRun.getFinishedAt() : testRun.getCreatedAt());
    }
}
