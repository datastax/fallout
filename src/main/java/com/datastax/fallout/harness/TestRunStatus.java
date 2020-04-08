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
package com.datastax.fallout.harness;

import java.util.function.Consumer;

import com.datastax.fallout.service.core.TestRun;

/**
 * Callbacks and blocking methods needed to monitor and synchronize with the {@link TestRun.State}.
 */
public interface TestRunStatus
{
    /** Blocks until the test run has either finished or reserved resources; if any
     *  listeners have been added with {@link #addResourcesUnavailableCallback} they must
     *  be called before completion. */
    void waitUntilInactiveOrResourcesChecked();

    /** Add a callback that must be run after the test run has completed
     *  with the state {@link TestRun.State#WAITING_FOR_RESOURCES} */
    void addResourcesUnavailableCallback(Runnable onResourcesUnavailable);

    /** Add a callback that must be run after the test run has become inactive */
    void addInactiveCallback(Runnable onInactive);

    /** Add a callback that must be run after the test has completed */
    void addFinishedCallback(Runnable onCompleted);

    /** Signal that the test should be aborted. */
    void abort();

    void addInactiveOrResourcesReservedCallback(Runnable onResourcesReserved);

    void addStateListener(Consumer<TestRun.State> onStateSet);

    void sendCurrentStateToStateListeners();
}
