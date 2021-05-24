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
package com.datastax.fallout.harness;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.service.core.TestRun;

/** Used to update the state of a TestRun, delegating actual storage
 *  of {@link TestRun.State} and {@link TestResult} to an implementation of {@link StateStorage}. */
public class TestRunAbortedStatusUpdater extends TestRunStatusUpdater implements TestRunAbortedStatus
{
    private Optional<TestRun.State> finalStateAfterFailure = Optional.empty();

    private volatile Optional<CompletableFuture<Void>> inactiveOrResourcesCheckedFuture = Optional.empty();
    private final List<Runnable> abortListeners = new ArrayList<>();
    private final List<Pair<Predicate<TestRun.State>, Runnable>> stateCallbacks = new ArrayList<>();

    private final StateStorage stateStorage;

    public interface StateStorage extends TestRunStatusUpdater.StateStorage
    {
        default void markFailedWithReason(TestRun.State finalState)
        {
        }

        default void markInactive(TestRun.State finalState, Optional<TestResult> testResult)
        {
            setCurrentState(finalState);
        }
    }

    public TestRunAbortedStatusUpdater(StateStorage stateStorage)
    {
        super(stateStorage);
        this.stateStorage = stateStorage;
    }

    /** Delegates implementation of getting the stored state to {@link StateStorage#getCurrentState} */
    @VisibleForTesting
    public synchronized TestRun.State getCurrentState()
    {
        return stateStorage.getCurrentState();
    }

    /** Records finalState (but only the first time its called), calls {@link StateStorage#markFailedWithReason} to do any
     *  storage-specific processing, then notifies any listeners registered using {@link #addAbortListener}. */
    public synchronized void markFailedWithReason(TestRun.State finalState)
    {
        Preconditions.checkArgument(finalState.failed());
        finalStateAfterFailure = Optional.of(finalStateAfterFailure.orElse(finalState));
        stateStorage.markFailedWithReason(finalState);

        if (finalState == TestRun.State.ABORTED)
        {
            notifyAbortListeners();
        }
    }

    /** Calls {@link StateStorage#markInactive} to store the testResult and {@link
     *  #setCurrentState} to set the final state depending on the testResult; however, if
     *  {@link #markFailedWithReason} has been called, then the state set there will be used */
    public synchronized void markInactive(Optional<TestResult> testResult)
    {
        final TestRun.State finalState = finalStateAfterFailure.orElse(
            testResult.map(TestResult::isValid).orElse(false) ?
                TestRun.State.PASSED :
                TestRun.State.FAILED);
        stateStorage.markInactive(finalState, testResult);
        handleState(finalState);
    }

    @Override
    public synchronized boolean hasBeenAborted()
    {
        return finalStateAfterFailure.map(state -> state == TestRun.State.ABORTED).orElse(false);
    }

    @Override
    public synchronized boolean canBeInterruptedWithAbort()
    {
        return stateStorage.getCurrentState() == TestRun.State.RUNNING ||
            stateStorage.getCurrentState() == TestRun.State.SETTING_UP;
    }

    private void notifyAbortListeners()
    {
        abortListeners.forEach(Runnable::run);
    }

    @Override
    public synchronized void addAbortListener(Runnable onAbort)
    {
        abortListeners.add(onAbort);
    }

    @Override
    public void abort()
    {
        markFailedWithReason(TestRun.State.ABORTED);
    }
}
