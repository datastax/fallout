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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ScopedLogger;

/** Implementation of {@link RunnableExecutorFactory} that tracks active TestRuns and allows them to be aborted */
public class AbortableRunnableExecutorFactory implements RunnableExecutorFactory, DelegatingExecutorFactory.ExecutorPool
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(AbortableRunnableExecutorFactory.class);

    private final RunnableExecutorFactory executorFactory;

    private Map<UUID, Executor> activeTestRuns = new ConcurrentHashMap<>();

    /** Create instance that delegates to executorFactory */
    public AbortableRunnableExecutorFactory(RunnableExecutorFactory executorFactory)
    {
        this.executorFactory = executorFactory;
    }

    private <E extends Executor> E registerRemoveOnInactiveCallback(UUID testrunId, E executor)
    {
        executor.getTestRunStatus().addInactiveCallback(() -> activeTestRuns.remove(testrunId));
        return executor;
    }

    /** Calls create on the delegated executorFactory */
    @Override
    public RunnableExecutor create(TestRun testRun, UserCredentialsFactory.UserCredentials userCredentials)
    {
        final var executor = executorFactory.create(testRun, userCredentials);
        activeTestRuns.put(testRun.getTestRunId(),
            registerRemoveOnInactiveCallback(testRun.getTestRunId(), executor));
        return executor;
    }

    @Override
    public void addExecutorIfNotExists(UUID testRunId, Supplier<Executor> createExecutor)
    {
        activeTestRuns.computeIfAbsent(testRunId,
            ignored -> registerRemoveOnInactiveCallback(testRunId, createExecutor.get()));
    }

    @Override
    public void close()
    {
        executorFactory.close();
    }

    public boolean abort(UUID testRunId)
    {
        final var executor = activeTestRuns.get(testRunId);
        if (executor != null)
        {
            executor.getTestRunStatus().abort();
            return true;
        }
        return false;
    }

    public List<TestRun> abortAndCopy()
    {
        return activeTestRuns.values().stream()
            .map(executor -> {
                executor.getTestRunStatus().abort();
                return executor.getTestRunCopyForReRun();
            })
            .collect(Collectors.toList());
    }

    public List<ReadOnlyTestRun> activeTestRuns()
    {
        return activeTestRuns.values().stream()
            .map(Executor::getReadOnlyTestRun)
            .collect(Collectors.toList());
    }

    /** Synchronously send the current state to all listeners; once this has returned, we're guaranteed
     *  all the listeners have been notified. */
    public void sendCurrentTestRunStatusToListeners()
    {
        activeTestRuns.values().stream().map(Executor::getTestRunStatus)
            .forEach(TestRunStatus::sendCurrentStateToStateListeners);
    }
}
