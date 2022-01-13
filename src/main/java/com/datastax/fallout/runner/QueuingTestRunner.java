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
package com.datastax.fallout.runner;

import java.time.Duration;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.runner.queue.PendingQueue;
import com.datastax.fallout.runner.queue.TestRunQueue;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.NamedThreadFactory;

/**
 *  Main entry point from the fallout UI to managing all the mechanics of queuing, viewing, and aborting test runs.
 *
 *  <p> Uses a {@link TestRunQueueProcessor} to process the provided {@link TestRunQueue} by using the provided
 *  {@link RunnableExecutorFactory} to create {@link RunnableExecutorFactory.RunnableExecutor}s from {@link TestRun}s.
 *
 *  <p> Provides views onto the currently running and queued tests,
 *  and means to {@link #abortTestRun}s and {@link #requestShutdown}s.
 */
public class QueuingTestRunner implements AutoCloseable, Managed
{
    private static final Logger logger = LoggerFactory.getLogger(TestRunQueueProcessor.class);
    private final Consumer<TestRun> testRunUpdater;
    private final Consumer<TestRun> testUpdater;
    private final TestRunQueue testRunQueue;

    private final AbortableRunnableExecutorFactory abortableTestRunExecutorFactory;
    private final TestRunQueueProcessor testRunQueueProcessor;
    private final ExecutorService testRunQueueProcessorExecutor =
        Executors.newSingleThreadExecutor(new NamedThreadFactory("TestRunQueue"));
    private final ResourceLimiter resourceLimiter;
    private Function<TestRun, Set<ResourceRequirement>> getResourceRequirements = testRun -> Set.of();

    private enum ShutdownState
    {
        RUNNING, SHUTDOWN_REQUESTED, TESTRUNS_ABORTED_AND_REQUEUED
    }

    private class ShutdownStateHandler
    {
        private ShutdownState shutdownState = ShutdownState.RUNNING;
        private String shutdownRequester = "";

        synchronized void requestShutdown(String requester)
        {
            shutdownState = ShutdownState.SHUTDOWN_REQUESTED;
            shutdownRequester = requester;
            testRunQueue.pause();
        }

        synchronized void cancelShutdown()
        {
            shutdownState = ShutdownState.RUNNING;
            shutdownRequester = "";
            testRunQueue.resume();
        }

        synchronized List<TestRun> abortAndRequeueRunningTestRuns()
        {
            Preconditions.checkState(shutdownState == ShutdownState.SHUTDOWN_REQUESTED,
                "shutdown must be requested before abort and requeue");

            List<TestRun> requeued = abortableTestRunExecutorFactory.abortAndCopy();
            requeued.forEach(QueuingTestRunner.this::queueTestRun);
            return requeued;
        }

        synchronized ShutdownState shutdownState()
        {
            return shutdownState;
        }

        synchronized String getShutdownRequester()
        {
            return shutdownRequester;
        }
    }

    private final ShutdownStateHandler shutdownStateHandler = new ShutdownStateHandler();

    public QueuingTestRunner(Consumer<TestRun> testRunUpdater,
        Consumer<TestRun> testUpdater,
        PendingQueue pendingQueue,
        UserCredentialsFactory userCredentialsFactory,
        AbortableRunnableExecutorFactory abortableTestRunExecutorFactory,
        Function<TestRun, Set<ResourceRequirement>> getResourceRequirements,
        ResourceReservationLocks resourceReservationLocks,
        List<ResourceLimit> resourceLimits,
        boolean startPaused)
    {
        this.testRunUpdater = testRunUpdater;
        this.testUpdater = testUpdater;
        this.abortableTestRunExecutorFactory = abortableTestRunExecutorFactory;
        this.getResourceRequirements = getResourceRequirements;
        resourceLimiter = new ResourceLimiter(
            () -> TestRun.getResourceRequirementsForTestRuns(getRunningTestRuns()), resourceLimits);

        testRunQueue = new TestRunQueue(pendingQueue,
            this::handleProcessingException,
            this::getRunningTestRuns,
            Duration.ofMinutes(1),
            resourceLimiter.and(resourceReservationLocks::couldAcquire));
        testRunQueueProcessor =
            new TestRunQueueProcessor(testRunQueue, userCredentialsFactory, abortableTestRunExecutorFactory,
                resourceReservationLocks);

        if (startPaused)
        {
            requestShutdown("startPaused configuration option");
        }
    }

    @VisibleForTesting
    public QueuingTestRunner(Consumer<TestRun> testRunUpdater,
        TestRunQueue testRunQueue,
        UserCredentialsFactory userCredentialsFactory,
        RunnableExecutorFactory executorFactory,
        Function<TestRun, Set<ResourceRequirement>> getResourceRequirements,
        ResourceReservationLocks resourceReservationLocks)
    {
        this.testRunUpdater = testRunUpdater;
        this.testUpdater = testRun -> {};
        abortableTestRunExecutorFactory = new AbortableRunnableExecutorFactory(executorFactory);
        this.getResourceRequirements = getResourceRequirements;
        resourceLimiter = new ResourceLimiter(
            () -> TestRun.getResourceRequirementsForTestRuns(getRunningTestRuns()), List.of());

        this.testRunQueue = testRunQueue;
        testRunQueueProcessor =
            new TestRunQueueProcessor(testRunQueue, userCredentialsFactory, abortableTestRunExecutorFactory,
                resourceReservationLocks);

        Exceptions.runUnchecked(this::start);
    }

    @Override
    public void start() throws Exception
    {
        testRunQueueProcessorExecutor.execute(testRunQueueProcessor);
    }

    @Override
    public void stop() throws Exception
    {
        close();
    }

    @Override
    public void close()
    {
        testRunQueueProcessor.shutdown();
        testRunQueueProcessorExecutor.shutdown();
        try
        {
            testRunQueueProcessorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
        }
        catch (InterruptedException e)
        {
            logger.error("Interrupted while waiting for TestRunner executor to terminate", e);
        }
    }

    /** @see TestRunQueue#getQueuedTestRuns
     * @return */
    public List<ReadOnlyTestRun> getQueuedTestRuns()
    {
        return testRunQueue.getQueuedTestRuns();
    }

    private List<ReadOnlyTestRun> getRunningTestRuns()
    {
        return abortableTestRunExecutorFactory.activeTestRuns();
    }

    public Predicate<Test> createHasUnfinishedTestRunsPredicate()
    {
        final var testRuns = Stream
            .concat(abortableTestRunExecutorFactory.activeTestRuns().stream(), getQueuedTestRuns().stream())
            .toList();

        return test -> testRuns.stream().anyMatch(testRun -> testRun.belongsTo(test));
    }

    /** Return the running TestRuns, ordered by duration from shortest to longest
     * @return*/
    public List<ReadOnlyTestRun> getRunningTestRunsOrderedByDuration()
    {
        return getRunningTestRuns()
            .stream()
            .sorted(Comparator.comparing(ReadOnlyTestRun::getStartedAt).reversed())
            .toList();
    }

    public int getRunningTestRunsCount()
    {
        return abortableTestRunExecutorFactory.activeTestRuns().size();
    }

    public void requestShutdown(String requester)
    {
        shutdownStateHandler.requestShutdown(requester);
    }

    public void cancelShutdown()
    {
        shutdownStateHandler.cancelShutdown();
    }

    public List<TestRun> abortAndRequeueRunningTestRuns()
    {
        return shutdownStateHandler.abortAndRequeueRunningTestRuns();
    }

    public boolean isShutdownRequested()
    {
        return shutdownStateHandler.shutdownState().ordinal() >= ShutdownState.SHUTDOWN_REQUESTED.ordinal();
    }

    public boolean testsHaveBeenAbortedAndRequeued()
    {
        return shutdownStateHandler.shutdownState() == ShutdownState.TESTRUNS_ABORTED_AND_REQUEUED;
    }

    private String getShutdownRequester()
    {
        return shutdownStateHandler.getShutdownRequester();
    }

    public Optional<String> getShutdownRequestMessage()
    {
        return isShutdownRequested() ?
            Optional.of("Fallout shutdown requested by " + getShutdownRequester() +
                " - new test runs will be queued but not processed") :
            Optional.empty();
    }

    public boolean queueTestRun(TestRun testRun)
    {
        testRun.setCreatedAt(new Date());
        testRun.setResourceRequirements(getResourceRequirements.apply(testRun));
        testRunUpdater.accept(testRun);
        testUpdater.accept(testRun);
        testRunQueue.add(testRun);
        return true;
    }

    private void handleProcessingException(TestRun testRun, Throwable ex)
    {
        final var message = String.format("Unexpected exception processing %s", testRun.getShortName());
        logger.error(message, ex);

        testRun.setFinishedAt(new Date());
        testRun.setFailedDuring(testRun.getState());
        testRun.setState(TestRun.State.FAILED);
        testRun.setResults(message + "; see server log for details: " + ex.getMessage());

        testRunUpdater.accept(testRun);
    }

    public boolean abortTestRun(TestRun testRun)
    {
        if (testRunQueue.remove(testRun))
        {
            testRun.setState(TestRun.State.ABORTED);
            testRunUpdater.accept(testRun);
            return true;
        }

        return abortableTestRunExecutorFactory.abort(testRun.getTestRunId());
    }
}
