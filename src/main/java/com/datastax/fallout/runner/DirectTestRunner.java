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

import java.util.UUID;

import io.dropwizard.lifecycle.Managed;

import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ScopedLogger;

/** Main entry point for the fallout runner process */
public class DirectTestRunner implements Managed
{
    private final AbortableRunnableExecutorFactory abortableExecutorFactory;
    private final AsyncShutdownHandler shutdownHandler;
    private final TestRunStatusUpdatePublisher testRunStatusUpdatePublisher;

    private static final ScopedLogger logger = ScopedLogger.getLogger(DirectTestRunner.class);

    public DirectTestRunner(RunnableExecutorFactory executorFactory, Runnable shutdownHandler,
        TestRunStatusUpdatePublisher testRunStatusUpdatePublisher)
    {
        this.abortableExecutorFactory = new AbortableRunnableExecutorFactory(executorFactory);
        this.shutdownHandler = new AsyncShutdownHandler(shutdownHandler);
        this.testRunStatusUpdatePublisher = testRunStatusUpdatePublisher;
    }

    public synchronized boolean run(TestRun testRun, UserCredentials userCredentials)
    {
        if (!shutdownHandler.isShuttingDown())
        {
            final RunnableExecutorFactory.RunnableExecutor executor =
                abortableExecutorFactory.create(testRun, userCredentials);
            final var testRunId = testRun.getTestRunIdentifier();
            executor.getTestRunStatus().addStateListener(state -> testRunStatusUpdatePublisher.publish(
                new TestRunStatusUpdate(testRunId, state)));
            executor.run();
        }
        return !shutdownHandler.isShuttingDown();
    }

    public synchronized boolean abort(UUID testRunId)
    {
        return abortableExecutorFactory.abort(testRunId);
    }

    public synchronized void startShutdown()
    {
        shutdownHandler.startShutdown();
    }

    public int getRunningTestRunsCount()
    {
        return abortableExecutorFactory.activeTestRuns().size();
    }

    public void publishCurrentTestRunStatus()
    {
        abortableExecutorFactory.sendCurrentTestRunStatusToListeners();
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
        abortableExecutorFactory.close();
    }
}
