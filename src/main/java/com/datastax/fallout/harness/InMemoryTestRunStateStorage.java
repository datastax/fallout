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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.core.TestRun;

public class InMemoryTestRunStateStorage implements TestRunAbortedStatusUpdater.StateStorage
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryTestRunStateStorage.class);

    private TestRun.State currentState;

    public InMemoryTestRunStateStorage(TestRun.State initialState)
    {
        currentState = initialState;
    }

    @Override
    public synchronized TestRun.State getCurrentState()
    {
        return currentState;
    }

    @Override
    public synchronized void setCurrentState(TestRun.State state)
    {
        logger.debug("{} -> {}", currentState, state);
        currentState = state;
    }
}
