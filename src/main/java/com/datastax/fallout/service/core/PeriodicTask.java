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
package com.datastax.fallout.service.core;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import io.dropwizard.lifecycle.Managed;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.LockHolder;
import com.datastax.fallout.util.ScopedLogger;

/** Simple periodic task that can be paused.  */
public abstract class PeriodicTask implements Managed
{
    private final boolean startPaused;
    private final HashedWheelTimer timer;
    private final Duration delay;
    private final Duration repeat;
    private final ReentrantLock runningTaskLock;

    private volatile Timeout timeout;

    /** Create a task that will run the first time after the specified delay,
     *  then subsequently every repeat interval.
     *
     *  Tasks will acquire the lock before running, as will external calls via {@link #runExclusively};
     *  this means that the timer dispatch thread will be blocked while running.  This allows the
     *  timer to queue up other tasks that have triggered while this task was running.  The
     *  alternative would be to skip tasks that can't be run, but that could leave to starvation. */
    PeriodicTask(boolean startPaused, HashedWheelTimer timer, ReentrantLock runningTaskLock,
        Duration delay, Duration repeat)
    {
        this.startPaused = startPaused;
        this.timer = timer;
        this.delay = delay;
        this.repeat = repeat;
        this.runningTaskLock = runningTaskLock;
    }

    protected abstract ScopedLogger logger();

    protected abstract void runTask();

    public synchronized void pause()
    {
        if (timeout != null)
        {
            logger().info("Pausing");
            timeout.cancel();
            timeout = null;
        }
    }

    protected boolean isPaused()
    {
        return timeout == null;
    }

    private synchronized void scheduleFirstRun()
    {
        timeout = timer.newTimeout(this::runTaskAndReschedule, delay.toSeconds(), TimeUnit.SECONDS);
    }

    private void runTaskAndReschedule(Timeout timeout)
    {
        runExclusively(this::runTask);
        reschedule();
    }

    private synchronized void reschedule()
    {
        if (isPaused())
        {
            return;
        }

        timeout = timer.newTimeout(this::runTaskAndReschedule, repeat.toSeconds(), TimeUnit.SECONDS);
    }

    public synchronized void run()
    {
        if (isPaused())
        {
            logger().info("Running");
            scheduleFirstRun();
        }
    }

    protected void runExclusively(Runnable exclusive)
    {
        try (var lockHolder = LockHolder.acquire(runningTaskLock))
        {
            exclusive.run();
        }
    }

    @Override
    public void start()
    {
        if (!startPaused)
        {
            run();
        }
    }

    @Override
    public void stop()
    {
    }
}
