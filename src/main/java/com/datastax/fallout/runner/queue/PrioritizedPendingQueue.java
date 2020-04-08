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
    private final PendingQueue pendingQueue;
    private final Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier;
    private final Predicate<ReadOnlyTestRun> available;

    @VisibleForTesting
    public PrioritizedPendingQueue(PendingQueue pendingQueue, Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier,
        Predicate<ReadOnlyTestRun> available)
    {
        this.pendingQueue = pendingQueue;
        this.runningTestRunsSupplier = runningTestRunsSupplier;
        this.available = available;
    }

    public void add(TestRun testRun)
    {
        pendingQueue.add(testRun);
    }

    public boolean remove(TestRun testRun)
    {
        return pendingQueue.remove(testRun);
    }

    public Optional<TestRun> remove()
    {
        if (noneAvailable())
        {
            return Optional.empty();
        }

        TestRun testRun = pending().stream()
            .filter(available)
            .findFirst()
            .get();

        pendingQueue.remove(testRun);

        return Optional.of(testRun);
    }

    public List<TestRun> pending()
    {
        return pendingQueue.pending().stream()
            .sorted(comparator(runningTestRunsSupplier.get()))
            .collect(Collectors.toList());
    }

    public boolean noneAvailable()
    {
        return pendingQueue.pending().stream().noneMatch(available);
    }

    /**
     * Returns a comparator that will order {@link TestRun}s from highest to lowest priority according to the following
     * rules:
     *
     * <ol>
     * <li> Testruns with an owner that already has tests in runningTestRuns will be given a lower priority
     * than testruns with an owner that has no tests in runningTestRuns.  The more tests owned in runningTestRuns, the
     * lower the priority.
     * <li> Then by finished time; if no finished time, the created time: the older, the higher.
     * </ol>
     * @param runningTestRuns
     */
    public static Comparator<TestRun> comparator(List<ReadOnlyTestRun> runningTestRuns)
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
