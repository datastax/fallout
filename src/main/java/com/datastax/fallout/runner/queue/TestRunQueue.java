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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ScopedLogger;

/**
 * Implementation of {@link ReadOnlyTestRunQueue} that differentiates between newly added {@link TestRun}s and
 * {@link TestRun}s that have been re-added.
 *
 * <ol>
 *   <li> Adding new {@link TestRun}s will cause any blocking call to take to unblock immediately.
 *   <li> If there are no new {@link TestRun}s at the end of a call to take, the next call to take will block
 *        for a regular interval.
 * </ol>
 */
public class TestRunQueue implements ReadOnlyTestRunQueue
{
    @VisibleForTesting
    protected static final ScopedLogger logger = ScopedLogger.getLogger(TestRunQueue.class);

    @VisibleForTesting
    public static Predicate<ReadOnlyTestRun>
        doesNotRequireClusterInUse(Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier)
    {
        return testRun -> {
            Set<String> requiredClusters = testRun.getResourceRequirements().stream()
                .map(resourceRequirement -> resourceRequirement.getResourceType().getUniqueName())
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toSet());

            return requiredClusters.isEmpty() || runningTestRunsSupplier.get().stream()
                .flatMap(_testRun -> _testRun.getResourceRequirements().stream())
                .map(resourceRequirement -> resourceRequirement.getResourceType().getUniqueName())
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .noneMatch(requiredClusters::contains);
        };
    }

    public final static class Blocker
    {
        private static final ScopedLogger logger = ScopedLogger.getLogger(Blocker.class);

        private final Optional<Duration> temporaryDelay;
        boolean blocked = false;

        /** Create a blocker whose {@link #blockTemporarily()} will block for <code>temporaryDelay</code>
         *  or until {@link #unblock()} is called. */
        public Blocker(Duration temporaryDelay)
        {
            this.temporaryDelay = Optional.of(temporaryDelay);
        }

        /** Create a blocker which makes {@link #blockTemporarily()} behave the same as {@link #block()} */
        @VisibleForTesting
        public Blocker()
        {
            this.temporaryDelay = Optional.empty();
        }

        public boolean testRunNotTriedRecently(ReadOnlyTestRun testRun)
        {
            return temporaryDelay
                .map(temporaryDelay_ -> {
                    Date oneDurationAgo = Date.from(Instant.now().minus(temporaryDelay_));
                    return testRun.getFinishedAt() == null || testRun.getFinishedAt().before(oneDurationAgo);
                })
                .orElse(true);
        }

        private synchronized void block(Optional<Duration> delay)
        {
            blocked = true;
            notifyAll();
            while (blocked)
            {
                if (delay.isPresent())
                {
                    // Stop blocking after a single uninterrupted wait
                    Exceptions.runUninterruptibly(() -> wait(delay.get().toMillis()));
                    blocked = false;
                }
                else
                {
                    Exceptions.runUninterruptibly(this::wait);
                }
            }
        }

        public synchronized void blockTemporarily()
        {
            logger.doWithScopedDebug(() -> block(temporaryDelay), "blockTemporarily");
        }

        public synchronized void block()
        {
            logger.doWithScopedDebug(() -> block(Optional.empty()), "block");
        }

        public synchronized void unblock()
        {
            logger.doWithScopedDebug(() -> {
                blocked = false;
                notifyAll();
            }, "unblock");
        }

        @VisibleForTesting
        public synchronized void waitUntilBlocked()
        {
            logger.doWithScopedDebug(() -> {
                while (!blocked)
                {
                    Exceptions.runUninterruptibly(this::wait);
                }
            }, "waitUntilBlocked");
        }
    }

    private static class SynchronizedTestRunQueue
    {
        private final PrioritizedPendingQueue prioritizedPendingQueue;

        // The currently processing testrun.  This is set for the duration of take()'s consumer,
        // and when remove is called we check this first (remove won't work on the currently
        // processing testrun).  Once the consumer has finished and take has returned then we can
        // assume that testrun will either turn up in the processing or pending queues and can be
        // removed, or it will have continued onwards to run, and should be aborted by other means.
        private Optional<TestRun> currentlyProcessing = Optional.empty();

        // Used to implement blocking, this also acts as the synchronization guard.  Calling
        // blocker.block() will release the lock, since block() calls blocker.wait().
        private final Blocker blocker;

        private boolean paused = false;

        private SynchronizedTestRunQueue(Blocker blocker, PrioritizedPendingQueue prioritizedPendingQueue)
        {
            this.blocker = blocker;
            this.prioritizedPendingQueue = prioritizedPendingQueue;
        }

        private void logState()
        {
            synchronized (blocker)
            {
                logger.debug("prioritizedPendingQueue.size={}  paused={}",
                    prioritizedPendingQueue.pending().size(), paused);
            }
        }

        private void waitForUnblockIfNoTestRunsForProcessingOrPaused()
        {
            synchronized (blocker)
            {
                logger.doWithScopedDebug(() -> {
                    logState();
                    if (paused)
                    {
                        blocker.block();
                    }
                    else if (prioritizedPendingQueue.noneAvailable())
                    {
                        blocker.blockTemporarily();
                    }
                    logState();
                }, "waitForUnblockIfNoTestRunsForProcessingOrPaused");
            }
        }

        /**
         * Check to see if there's a testrun waiting; it's possible there
         * isn't, because we could have been unblocked with an empty queue.
         */
        private Optional<TestRun> getTestRunForProcessing()
        {
            synchronized (blocker)
            {
                return logger.doWithScopedDebug(() -> {
                    logState();
                    if (prioritizedPendingQueue.noneAvailable() || paused)
                    {
                        logger.debug("Nothing to process");
                        currentlyProcessing = Optional.empty();
                    }
                    else
                    {
                        Optional<TestRun> testRunToBeProccessed = prioritizedPendingQueue.remove();
                        testRunToBeProccessed.ifPresent(testRun -> {
                            logger.info("Processing ({})", testRun.getShortName());
                            currentlyProcessing = Optional.of(testRun);
                        });
                    }

                    return currentlyProcessing;
                }, "getTestRunForProcessing");
            }
        }

        private void requeueTestRun(TestRun testRun)
        {
            synchronized (blocker)
            {
                logger.info("Requeueing ({})", testRun.getShortName());
                prioritizedPendingQueue.add(testRun);
            }
        }

        private void finishProcessingTestRun()
        {
            synchronized (blocker)
            {
                currentlyProcessing
                    .ifPresent(testRun -> logger.info("Finished processing ({})", testRun.getShortName()));
                currentlyProcessing = Optional.empty();
            }
        }

        private void addNewTestRunAndUnblock(TestRun testRun)
        {
            synchronized (blocker)
            {
                logger.info("Adding ({})", testRun.getShortName());
                prioritizedPendingQueue.add(testRun);
                unblock();
            }
        }

        private void unblock()
        {
            synchronized (blocker)
            {
                logger.info("Unblocking queue");
                blocker.unblock();
            }
        }

        private void pause()
        {
            synchronized (blocker)
            {
                logger.info("Pausing queue");
                paused = true;
            }
        }

        private void resumeAndUnblock()
        {
            synchronized (blocker)
            {
                logger.info("Resuming queue");
                paused = false;
                unblock();
            }
        }

        private List<ReadOnlyTestRun> getQueuedTestRuns()
        {
            synchronized (blocker)
            {
                List<ReadOnlyTestRun> pendingTestRuns = new ArrayList<>(prioritizedPendingQueue.pending());
                Collections.reverse(pendingTestRuns);
                return pendingTestRuns;
            }
        }

        private boolean remove(TestRun testRun)
        {
            synchronized (blocker)
            {
                try (ScopedLogger.Scoped ignored = logger.scopedInfo("Removing (" + testRun.getShortName() + ")"))
                {
                    if (currentlyProcessing
                        .map(testRun_ -> testRun_.getTestRunId().equals(testRun.getTestRunId()))
                        .orElse(false))
                    {
                        logger.info("Not removing test run from currentlyProcessing");
                        return false;
                    }
                    if (prioritizedPendingQueue.remove(testRun))
                    {
                        logger.info("Removed test run from pendingQueue");
                        return true;
                    }
                    logger.info("Test run not found");
                    return false;
                }
            }
        }
    }

    private final SynchronizedTestRunQueue queue;

    public TestRunQueue(PendingQueue pendingQueue, Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier,
        Duration retryTestRunAfter, Predicate<ReadOnlyTestRun> available)
    {
        this(pendingQueue, runningTestRunsSupplier, new Blocker(retryTestRunAfter), available);
    }

    @VisibleForTesting
    public TestRunQueue(PendingQueue pendingQueue, Supplier<List<ReadOnlyTestRun>> runningTestRunsSupplier,
        Blocker blocker, Predicate<ReadOnlyTestRun> available)
    {
        this.queue = new SynchronizedTestRunQueue(blocker,
            new PrioritizedPendingQueue(pendingQueue, runningTestRunsSupplier,
                available.and(blocker::testRunNotTriedRecently)
                    .and(doesNotRequireClusterInUse(runningTestRunsSupplier))));
    }

    public void add(TestRun testRun)
    {
        queue.addNewTestRunAndUnblock(testRun);
    }

    @Override
    public void take(BiConsumer<TestRun, Consumer<TestRun>> consumer)
    {
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("take"))
        {
            queue.waitForUnblockIfNoTestRunsForProcessingOrPaused();
            queue.getTestRunForProcessing().ifPresent(testRun -> consumer.accept(testRun, queue::requeueTestRun));
            queue.finishProcessingTestRun();
        }
    }

    public boolean remove(TestRun testRun)
    {
        return queue.remove(testRun);
    }

    @Override
    public void unblock()
    {
        queue.unblock();
    }

    /** Return the queued TestRuns, in reverse order of processing: the last in the list will be processed first */
    public List<ReadOnlyTestRun> getQueuedTestRuns()
    {
        return queue.getQueuedTestRuns();
    }

    public void pause()
    {
        queue.pause();
    }

    public void resume()
    {
        queue.resumeAndUnblock();
    }
}
