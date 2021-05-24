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
package com.datastax.fallout.util;

import java.util.concurrent.locks.Lock;

/** Utility try-with-resources class for acquiring {@link Lock} instances */
public class LockHolder<L extends Lock> implements AutoCloseable
{
    private final L lock;

    private LockHolder(L lock)
    {
        this.lock = lock;
        lock.lock();
    }

    public static <L extends Lock> LockHolder<L> acquire(L lockable)
    {
        return new LockHolder<>(lockable);
    }

    @Override
    public void close()
    {
        lock.unlock();
    }
}
