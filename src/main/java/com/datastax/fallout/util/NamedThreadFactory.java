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

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is an implementation of the <i>ThreadFactory</i> interface. This
 * is useful to give Java threads meaningful names which is useful when using
 * a tool like JConsole.
 */

public class NamedThreadFactory implements ThreadFactory
{
    private final String id;
    private final int priority;
    private final AtomicInteger n = new AtomicInteger(1);

    public NamedThreadFactory(String id)
    {
        this(id, Thread.NORM_PRIORITY);
    }

    public NamedThreadFactory(String id, int priority)
    {

        this.id = id;
        this.priority = priority;
    }

    public Thread newThread(Runnable runnable)
    {
        String name = id + ":" + n.getAndIncrement();
        Thread thread = new Thread(runnable, name);
        thread.setPriority(priority);
        thread.setDaemon(true);
        return thread;
    }
}
