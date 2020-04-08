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

import javax.ws.rs.client.WebTarget;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;

/** Connects to a {@link com.datastax.fallout.service.FalloutConfiguration.ServerMode#RUNNER}
 *  that is already running {@link com.datastax.fallout.service.core.TestRun}s */
public class DelegatingExecutorFactory extends AbstractDelegatingExecutorFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DelegatingExecutorFactory.class);

    public interface ExecutorPool
    {
        /** If testRunId is not in the pool of active {@link TestRun}s, calls createExecutor and adds it to the pool */
        void addExecutorIfNotExists(UUID testRunId, Supplier<RunnableExecutorFactory.Executor> createExecutor);
    }

    private final ExecutorPool executorPool;
    private final TestRunStatusUpdatePublisher.Subscription handleTestRunStatusUpdateSubscription;
    private final CompletableFuture<Void> allExistingTestRunsKnown = new CompletableFuture<>();

    /** Create a new instance that will:
     *
     *  <ul>
     *    <li>monitor the <code>/status</code> feed on the specified runner;
     *    <li>create new {@link RunnableExecutorFactory.Executor}s on-the-fly using
     *    <code>createExecutorIfNotExists</code>; these will also receive updates from the
     *    <code>/status</code> feed.
     *  </ul>
     */
    public DelegatingExecutorFactory(WebTarget runner,
        Function<TestRunIdentifier, TestRun> getTestRun,
        ExecutorPool executorPool)
    {
        super(runner, getTestRun);
        this.executorPool = executorPool;

        handleTestRunStatusUpdateSubscription =
            getTestRunStatusUpdatePublisher().subscribe(this::handleTestRunStatusUpdate);
    }

    @Override
    public void stop()
    {
        handleTestRunStatusUpdateSubscription.cancel();
        super.stop();
    }

    @Override
    protected void markAllExistingTestRunsKnown()
    {
        logger.info("{}: All test run statuses received", getRunner().getUri());
        allExistingTestRunsKnown.complete(null);
        handleTestRunStatusUpdateSubscription.cancel();
    }

    /** Completes once we're sure we've got all remote testruns */
    public CompletableFuture<Void> waitUntilAllExistingTestRunsKnownAsync()
    {
        return allExistingTestRunsKnown;
    }

    private boolean handleTestRunStatusUpdate(TestRunStatusUpdate update)
    {
        executorPool.addExecutorIfNotExists(update.getTestRunIdentifier().getTestRunId(), () -> {
            logger.info("{}: Creating new Executor from {}", getRunner().getUri(), update);
            return new DelegatingExecutor(
                getRunner(),
                DelegatingExecutorFactory.this::getTestRun,
                getTestRunStatusUpdatePublisher(),
                update.getTestRunIdentifier(),
                update.getState());
        });
        return true;
    }
}
