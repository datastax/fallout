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
package com.datastax.fallout.runner.queue;

import java.util.Collection;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import com.datastax.fallout.service.core.TestRun;

public class PersistentPendingQueue implements PendingQueue
{
    private final Supplier<Collection<TestRun>> getQueued;

    public PersistentPendingQueue(Supplier<Collection<TestRun>> getQueued)
    {
        this.getQueued = getQueued;
    }

    @Override
    public void add(TestRun testRun)
    {
        // no-op; the testRun should already exist in testRunDAO and be in a waiting state
        Preconditions.checkState(testRun.getState().waiting());
    }

    @Override
    public Collection<TestRun> pending()
    {
        return getQueued.get();
    }

    @Override
    public boolean remove(TestRun testRun)
    {
        return testRun.getState().waiting();
    }
}
