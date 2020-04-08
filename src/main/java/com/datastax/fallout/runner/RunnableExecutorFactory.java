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

import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;

/** Abstracts the machinery used to run a {@link TestRun} */
public interface RunnableExecutorFactory extends AutoCloseable
{
    RunnableExecutor create(TestRun testRun, UserCredentials userCredentials);

    /** Stops the factory, blocking until all testruns have completed */
    @Override
    default void close()
    {
    }

    /** These methods can be called regardless of whether the {@link TestRun} is already
     *  running or not.  Explicitly used to control {@link TestRun}s managed by {@link
     *  AbortableRunnableExecutorFactory} and created by {@link DelegatingExecutorFactory}
     *  i.e. cases that don't need to {@link RunnableExecutor#run} a {@link TestRun} */
    interface Executor
    {
        TestRunStatus getTestRunStatus();

        TestRun getTestRunCopyForReRun();

        ReadOnlyTestRun getReadOnlyTestRun();
    }

    /** Extends {@link Executor} to allow starting the execution of a {@link TestRun} */
    interface RunnableExecutor extends Executor, Runnable
    {
    };
}
