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
package com.datastax.fallout.runner.queue;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.datastax.fallout.service.core.TestRun;

public interface ReadOnlyTestRunQueue
{
    /**
     * Remove one {@link TestRun} from the queue and feed it to consumer.  Blocks on empty queue,
     * and possibly other implementation-specific conditions.  If the {@link TestRun} should be requeued, consumer
     * should pass it to its second parameter.
     */
    public void take(BiConsumer<TestRun, Consumer<TestRun>> consumer);

    /**
     * Forcibly unblock any current call to {@link #take}
     */
    public void unblock();
}
