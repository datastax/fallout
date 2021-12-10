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

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.WebTarget;

import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.harness.InMemoryTestRunStateStorage;
import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.resources.runner.RunnerResource;

import static com.datastax.fallout.service.views.FalloutView.uriFor;

/** Has most of the functionality required for monitoring a {@link TestRun} running on a
 * {@link com.datastax.fallout.service.FalloutConfiguration.ServerMode#RUNNER} instance */
public class DelegatingExecutor implements RunnableExecutorFactory.Executor
{
    private static final Logger logger = LoggerFactory.getLogger(DelegatingExecutor.class);

    private final WebTarget runner;
    private final Supplier<TestRun> getTestRun;
    private final TestRunIdentifier testRunIdentifier;
    private final TestRunStatusUpdater testRunStatusUpdater;
    private final TestRunStatusUpdatePublisher testRunStatusUpdatePublisher;

    public DelegatingExecutor(
        WebTarget runner, Function<TestRunIdentifier, TestRun> getTestRun,
        TestRunStatusUpdatePublisher testRunStatusUpdatePublisher,
        TestRunIdentifier testRunIdentifier, TestRun.State initialState)
    {
        this.runner = runner;
        this.getTestRun = () -> getTestRun.apply(testRunIdentifier);
        this.testRunStatusUpdatePublisher = testRunStatusUpdatePublisher;
        this.testRunIdentifier = testRunIdentifier;
        testRunStatusUpdater = new TestRunStatusUpdater(new InMemoryTestRunStateStorage(initialState));
    }

    @Override
    public TestRunStatus getTestRunStatus()
    {
        return testRunStatusUpdater;
    }

    @Override
    public TestRun getTestRunCopyForReRun()
    {
        return getTestRun.get().copyForReRun();
    }

    @Override
    public ReadOnlyTestRun getReadOnlyTestRun()
    {
        final var testRun = getTestRun.get();
        // Limit FAL-1776 blast radius: log the fact that we got a null testrun
        // here, so that we can log the identifier, but let the caller handle it.
        if (testRun == null)
        {
            logger.error("getTestRun({}) unexpectedly returned null", testRunIdentifier);
        }
        return testRun;
    }

    protected class TestRunStatusUpdater extends com.datastax.fallout.harness.TestRunStatusUpdater
    {
        /** Uses {@link InMemoryTestRunStateStorage} instead of the DB-backed {@link TestRunStateStorage}, since
         *  the runner is responsible for updating the DB. */
        public TestRunStatusUpdater(InMemoryTestRunStateStorage testRunStateStorage)
        {
            super(testRunStateStorage);

            final TestRunStatusUpdatePublisher.Subscription subscription = testRunStatusUpdatePublisher
                .subscribe(update -> {
                    if (update.getTestRunIdentifier().getTestRunId().equals(testRunIdentifier.getTestRunId()))
                    {
                        logger.info("{}: Updating Executor from {}", runner.getUri(), update);
                        testRunStatusUpdater.setCurrentState(update.getState());
                    }
                    return true;
                });

            addInactiveCallback(subscription::cancel);
        }

        @Override
        public void abort()
        {
            final WebTarget abort = runner
                .path(uriFor(RunnerResource.class, "abort", testRunIdentifier.getTestRunId()).toString());

            try
            {
                logger.info("{}: Aborting testrun via {}", runner.getUri(), abort.getUri());
                abort.request().post(null, Void.class);
            }
            catch (ProcessingException ex)
            {
                throw new RuntimeException(String.format("Could not abort testrun %s via %s",
                    testRunIdentifier.getTestRunId(), abort.getUri()), ex);
            }
        }
    }
}
