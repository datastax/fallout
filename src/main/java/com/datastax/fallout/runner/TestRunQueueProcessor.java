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
package com.datastax.fallout.runner;

import java.time.Duration;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.harness.ExceptionHandler;
import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.runner.queue.ReadOnlyTestRunQueue;
import com.datastax.fallout.service.core.TestRun;

/** Responsible for processing {@link TestRun}s in the provided testRunQueue.  It does this by repeatedly:
 *  <ul>
 *    <li> removing a {@link TestRun} from a testRunQueue using {@link ReadOnlyTestRunQueue#take}
 *    <li> using the supplied {@link RunnableExecutorFactory} to turn it into an {@link RunnableExecutorFactory.RunnableExecutor}
 *    <li> calling {@link RunnableExecutorFactory.RunnableExecutor#run} to process it
 *    <li> ...and waiting until the {@link TestRunStatus} indicates the test run has either acquired resources
 *         or finished
 *  </ul>
 */
public class TestRunQueueProcessor implements Runnable
{
    private static final Duration MAX_LOCK_DURATION = Duration.ofMinutes(5);
    private final ExceptionHandler exceptionHandler;
    private final ReadOnlyTestRunQueue testRunQueue;
    private final RunnableExecutorFactory executorFactory;
    private final ResourceReservationLocks resourceReservationLocks;

    private final UserCredentialsFactory userCredentialsFactory;

    private volatile boolean shutdownRequested = false;

    private static final Logger logger = LoggerFactory.getLogger(TestRunQueueProcessor.class);
    private static final Logger lockDurationLogger = LoggerFactory.getLogger(logger.getName() + ".LockDuration");

    TestRunQueueProcessor(ReadOnlyTestRunQueue testRunQueue,
        UserCredentialsFactory userCredentialsFactory, RunnableExecutorFactory executorFactory,
        ResourceReservationLocks resourceReservationLocks)
    {
        this(testRunQueue, userCredentialsFactory, logger::error, executorFactory, resourceReservationLocks);
    }

    @VisibleForTesting
    TestRunQueueProcessor(ReadOnlyTestRunQueue testRunQueue,
        UserCredentialsFactory userCredentialsFactory, ExceptionHandler exceptionHandler,
        RunnableExecutorFactory executorFactory, ResourceReservationLocks resourceReservationLocks)
    {
        this.testRunQueue = testRunQueue;
        this.userCredentialsFactory = userCredentialsFactory;
        this.exceptionHandler = exceptionHandler;
        this.executorFactory = executorFactory;
        this.resourceReservationLocks = resourceReservationLocks;
    }

    public void shutdown()
    {
        shutdownRequested = true;
        testRunQueue.unblock();
    }

    private void process(TestRun testRun, Runnable requeueJob)
    {
        RunnableExecutorFactory.RunnableExecutor executor =
            executorFactory.create(testRun, userCredentialsFactory.apply(testRun));

        final Optional<ResourceReservationLocks.Lock> lockedRequiredResources = resourceReservationLocks.tryAcquire(
            testRun);

        lockedRequiredResources.ifPresent(lockedRequiredResources_ -> {
            TestRunStatus testRunStatus = executor.getTestRunStatus();

            testRunStatus.addResourcesUnavailableCallback(requeueJob);
            testRunStatus.addInactiveOrResourcesReservedCallback(() -> {
                var lockDuration = lockedRequiredResources_.release();
                if (lockDuration.compareTo(MAX_LOCK_DURATION) > 0)
                {
                    lockDurationLogger.error("TestRun {} {} {} held {} for {}s, which is greater than {}s",
                        testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
                        lockedRequiredResources_.getLockedResources(),
                        lockDuration.getSeconds(), MAX_LOCK_DURATION.getSeconds());
                }
                else
                {
                    lockDurationLogger.info("TestRun {} {} {} held {} for {}s",
                        testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
                        lockedRequiredResources_.getLockedResources(),
                        lockDuration.getSeconds());
                }
            });

            executor.run();

            testRunStatus.waitUntilInactiveOrResourcesChecked();
        });

        if (!lockedRequiredResources.isPresent())
        {
            logger.warn("TestRunQueueProcessor.process: Couldn't acquire resource reservation lock for " +
                "testrun {} ({}); this shouldn't be possible, as ResourceReservationLocks.couldAcquire should " +
                "have been used to filter out the testrun before it reached this point",
                testRun.getTestRunId(), testRun.getResourceRequirements());
            requeueJob.run();
        }
    }

    @Override
    public void run()
    {
        while (!shutdownRequested)
        {
            testRunQueue.take((job, requeueJob) -> {
                try
                {
                    process(job, () -> requeueJob.accept(job));
                }
                catch (Exception e)
                {
                    exceptionHandler.accept("Unexpected exception in TestRunQueueProcessor", e);
                }
            });
        }

        executorFactory.close();
    }
}
