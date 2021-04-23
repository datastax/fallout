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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.TestRunAbortedStatusUpdater;
import com.datastax.fallout.harness.TestRunLinkUpdater;
import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.ops.JobLoggers;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.NamedThreadFactory;
import com.datastax.fallout.util.ScopedLogger;

public class ThreadedRunnableExecutorFactory implements RunnableExecutorFactory
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(ThreadedRunnableExecutorFactory.class);
    private final ExecutorService executorService =
        Executors.newCachedThreadPool(new NamedThreadFactory("TestRunExec"));
    private final FalloutConfiguration configuration;

    private final JobLoggersFactory loggersFactory;
    private final Consumer<TestRun> testRunUpdater;
    private final ActiveTestRunFactory activeTestRunFactory;

    public ThreadedRunnableExecutorFactory(
        JobLoggersFactory loggersFactory,
        Consumer<TestRun> testRunUpdater,
        ActiveTestRunFactory activeTestRunFactory,
        FalloutConfiguration configuration)
    {
        this.loggersFactory = loggersFactory;
        this.testRunUpdater = testRunUpdater;
        this.activeTestRunFactory = activeTestRunFactory;
        this.configuration = configuration;
    }

    private class ThreadedRunnableExecutor implements RunnableExecutorFactory.RunnableExecutor
    {
        private final UserCredentials userCredentials;
        private final JobLoggers loggers;
        private final AtomicallyPersistedTestRun testRun;
        private final TestRunAbortedStatusUpdater testRunStatus;
        private final TestRunLinkUpdater testRunLinkUpdater;

        private ThreadedRunnableExecutor(TestRun testRun, UserCredentials userCredentials)
        {
            this.userCredentials = userCredentials;
            loggers = loggersFactory.create(testRun, userCredentials);

            this.testRun = new AtomicallyPersistedTestRun(testRun, testRunUpdater);

            final Supplier<Map<String, Long>> findTestRunArtifacts = () -> this.testRun.get(
                testRun_ -> Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(configuration, testRun_)));

            testRunStatus = new TestRunAbortedStatusUpdater(
                new TestRunStateStorage(this.testRun, loggers.getShared(), findTestRunArtifacts));
            testRunLinkUpdater = testRunLinkUpdater(this.testRun);
            testRunStatus.addInactiveCallback(loggers::close);
            testRunStatus.addInactiveCallback(testRunLinkUpdater::removeLinks);
        }

        private TestRunLinkUpdater testRunLinkUpdater(AtomicallyPersistedTestRun testRun)
        {
            return new TestRunLinkUpdater() {
                @Override
                public void add(String linkName, String link)
                {
                    testRun.update(_testRun -> _testRun.addLink(linkName, link));
                }

                @Override
                public void removeLinks()
                {
                    testRun.update(TestRun::removeLinks);
                }
            };
        }

        @Override
        public TestRunStatus getTestRunStatus()
        {
            return testRunStatus;
        }

        @Override
        public TestRun getTestRunCopyForReRun()
        {
            return testRun.get(TestRun::copyForReRun);
        }

        @Override
        public ReadOnlyTestRun getReadOnlyTestRun()
        {
            return testRun.get(TestRun::immutableCopy);
        }

        @Override
        public void run()
        {
            activeTestRunFactory.create(testRun.get(Function.identity()), userCredentials, loggers, testRunStatus,
                testRunLinkUpdater)
                .ifPresent(ThreadedRunnableExecutorFactory.this::runAsync);
        }
    }

    @Override
    public RunnableExecutorFactory.RunnableExecutor create(TestRun testRun, UserCredentials userCredentials)
    {
        return new ThreadedRunnableExecutor(testRun, userCredentials);
    }

    @Override
    public void close()
    {
        if (!executorService.isTerminated())
        {
            executorService.shutdown();

            logger.withScopedInfo("Waiting for existing testruns to terminate").run(() -> {
                try
                {
                    executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
                }
                catch (InterruptedException e)
                {
                    logger.error("Interrupted while waiting for ThreadedExecutorFactory's executor to terminate", e);
                }
            });
        }
    }

    private void runAsync(ActiveTestRun activeTestRun)
    {
        try
        {
            CompletableFuture.runAsync(() -> {
                try
                {
                    activeTestRun.run(logger::error);
                }
                catch (Throwable t)
                {
                    // ActiveTestRun.run should not have allowed this to escape, but if such a thing does happen,
                    // we should at least attempt to log it.
                    logger.error("Unexpected exception running test run", t);
                }
            }, executorService);
        }
        catch (RejectedExecutionException e)
        {
            // We assume our ExecutorService handles an unbounded number of jobs:
            // the limit on jobs should be job resources, not capacity of the
            // executor.  If this assumption proves to be false and we start using a
            // bounded ExecutorService, then we'll need to stop failing tests here:

            try
            {
                logger.error("ThreadedExecutorFactory's executor unexpectedly rejected a job", e);
            }
            catch (Throwable ignored)
            {
                // squash logging exception as there is nothing we can do
            }
        }
    }
}
