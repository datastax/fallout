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
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.service.core.TestRun;

/** Used to update the state of a TestRun, delegating actual storage
 *  of {@link TestRun.State} to an implementation of {@link StateStorage}. */
public abstract class TestRunStatusUpdater implements TestRunStatus
{
    private volatile Optional<CompletableFuture<Void>> inactiveOrResourcesCheckedFuture = Optional.empty();
    private final List<Pair<Predicate<TestRun.State>, Runnable>> stateCallbacks = new ArrayList<>();
    private final List<Consumer<TestRun.State>> stateListeners = new ArrayList<>();

    private final StateStorage stateStorage;

    public interface StateStorage
    {
        TestRun.State getCurrentState();

        void setCurrentState(TestRun.State state);
    }

    public TestRunStatusUpdater(StateStorage stateStorage)
    {
        this.stateStorage = stateStorage;
    }

    private static boolean hasBecomeInactiveOrResourcesChecked(TestRun.State state)
    {
        return state != TestRun.State.CREATED && (!state.active() || state.resourcesChecked());
    }

    private static boolean hasBecomeInactiveOrResourcesReserved(TestRun.State state)
    {
        return state != TestRun.State.CREATED && (!state.active() || state.resourcesReserved());
    }

    /** Delegates implementation of storing the state to {@link StateStorage#setCurrentState},
     *  and handles firing the listeners and callbacks. */
    public synchronized void setCurrentState(TestRun.State state)
    {
        stateStorage.setCurrentState(state);
        handleState(state);
    }

    protected synchronized void handleState(TestRun.State state)
    {
        notifyStateCallbacks(state);

        if (hasBecomeInactiveOrResourcesChecked(state))
        {
            releaseInactiveOrResourcesCheckedFuture();
        }
    }

    @Override
    public synchronized void addResourcesUnavailableCallback(Runnable onResourcesUnavailable)
    {
        addStateCallback(state -> state == TestRun.State.WAITING_FOR_RESOURCES, onResourcesUnavailable);
    }

    @Override
    public synchronized void addInactiveCallback(Runnable onInactive)
    {
        addStateCallback(state -> !state.active(), onInactive);
    }

    @Override
    public void addActiveCallback(Runnable onActive)
    {
        addStateCallback(TestRun.State::active, onActive);
    }

    @Override
    public synchronized void addFinishedCallback(Runnable onFinished)
    {
        addStateCallback(TestRun.State::finished, onFinished);
    }

    @Override
    public synchronized void addInactiveOrResourcesReservedCallback(Runnable onResourcesReserved)
    {
        addStateCallback(TestRunStatusUpdater::hasBecomeInactiveOrResourcesReserved, onResourcesReserved);
    }

    private void releaseInactiveOrResourcesCheckedFuture()
    {
        inactiveOrResourcesCheckedFuture.ifPresent(future -> future.complete(null));
        inactiveOrResourcesCheckedFuture = Optional.empty();
    }

    /** This method is not synchronized because it can block. */
    @Override
    public void waitUntilInactiveOrResourcesChecked()
    {
        synchronized (this)
        {
            if (!hasBecomeInactiveOrResourcesChecked(stateStorage.getCurrentState()) &&
                !inactiveOrResourcesCheckedFuture.isPresent())
            {
                inactiveOrResourcesCheckedFuture = Optional.of(new CompletableFuture<>());
            }
        }

        inactiveOrResourcesCheckedFuture.ifPresent(future -> future.join());
    }

    private void addStateCallback(Predicate<TestRun.State> predicate, Runnable callback)
    {
        stateCallbacks.add(Pair.of(predicate, callback));
    }

    private List<Pair<Predicate<TestRun.State>, Runnable>> getAndRemoveCallbacksToRun(TestRun.State state)
    {
        var callbacksToRun = stateCallbacks.stream()
            .filter(pair -> pair.getLeft().test(state))
            .collect(Collectors.toList());

        stateCallbacks.removeIf(pair -> pair.getLeft().test(state));

        return callbacksToRun;
    }

    private void notifyStateCallbacks(TestRun.State state)
    {
        try
        {
            getAndRemoveCallbacksToRun(state).forEach(pair -> pair.getRight().run());
            notifyStateListeners(state);
        }
        catch (Exception e)
        {
            stateStorage.setCurrentState(TestRun.State.FAILED);
            throw new RuntimeException("Exception thrown in callback", e);
        }
    }

    private void notifyStateListeners(TestRun.State state)
    {
        // Copy the list to make sure we don't get concurrent modifications
        // from callbacks that add new listeners
        List.copyOf(stateListeners).forEach(listener -> listener.accept(state));
    }

    @Override
    public synchronized void addStateListener(Consumer<TestRun.State> onStateSet)
    {
        stateListeners.add(onStateSet);
    }

    @Override
    public synchronized void sendCurrentStateToStateListeners()
    {
        notifyStateListeners(stateStorage.getCurrentState());
    }
}
