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

import java.util.function.BiConsumer;

import com.datastax.fallout.service.core.TestRun;

public interface ReadOnlyTestRunQueue
{
    interface UnprocessedHandler
    {
        void requeue();

        void handleException(Throwable ex);
    }

    /**
     * Remove one {@link TestRun} from the queue and feed it to consumer.  Blocks on empty queue,
     * and possibly other implementation-specific conditions.  If the {@link TestRun} should
     * be requeued or an unexpected exception occurred, consumer is required to call {@link
     * UnprocessedHandler#requeue()} or {@link UnprocessedHandler#handleException} respectively.
     */
    void take(BiConsumer<TestRun, UnprocessedHandler> consumer);

    /**
     * Forcibly unblock any current call to {@link #take}
     */
    void unblock();
}
