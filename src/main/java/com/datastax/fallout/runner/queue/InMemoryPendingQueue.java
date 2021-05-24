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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.datastax.fallout.service.core.TestRun;

public class InMemoryPendingQueue implements PendingQueue
{
    private final List<TestRun> pendingQueue = new ArrayList<>();

    @Override
    public void add(TestRun testRun)
    {
        pendingQueue.add(testRun);
    }

    @Override
    public Collection<TestRun> pending()
    {
        return pendingQueue;
    }

    @Override
    public boolean remove(TestRun testRun)
    {
        return pendingQueue.removeIf(t -> t.getTestRunId().equals(testRun.getTestRunId()));
    }
}
