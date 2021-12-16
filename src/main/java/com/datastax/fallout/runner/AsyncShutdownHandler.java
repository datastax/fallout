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
package com.datastax.fallout.runner;

import java.util.concurrent.CompletableFuture;

import com.datastax.fallout.util.CompletableFutures;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ScopedLogger;

public class AsyncShutdownHandler
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(AsyncShutdownHandler.class);
    public static final Duration SHUTDOWN_GRACE_DELAY = Duration.seconds(5);

    private final Runnable shutdownHandler;
    private boolean shuttingDown = false;

    public AsyncShutdownHandler(Runnable shutdownHandler)
    {
        this.shutdownHandler = shutdownHandler;
    }

    public synchronized boolean isShuttingDown()
    {
        return shuttingDown;
    }

    /** Start a shutdown
     *
     *  This doesn't shut down immediately, as that would terminate the server before it could respond to
     *  this message.  Instead, it schedules an asynchronous job that waits for {@link #SHUTDOWN_GRACE_DELAY},
     *  shuts down its {@link RunnableExecutorFactory}, then calls the {@link #shutdownHandler} callback. */
    public synchronized void startShutdown()
    {
        if (shuttingDown)
        {
            logger.info("Already shutting down; ignoring request");
        }
        else
        {
            shuttingDown = true;
            logger.info("Shutting down in {}...", SHUTDOWN_GRACE_DELAY);
            CompletableFuture.runAsync(() -> {
                logger.withScopedInfo("Shutting down")
                    .run(() -> {
                        try
                        {
                            shutdownHandler.run();
                        }
                        catch (Throwable t)
                        {
                            logger.error("Exception in shutdown handler", t);
                        }
                    });
            },
                CompletableFutures.delayedExecutor(SHUTDOWN_GRACE_DELAY));
        }
    }
}
