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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorServices
{
    private ExecutorServices()
    {
        // utility class
    }

    private static int TERMINATION_TIMEOUT_SECONDS = 5;

    /** Does what it says with a timeout of {@link #TERMINATION_TIMEOUT_SECONDS}; {@link ExecutorService#shutdownNow}
     *  will interrupt the executing tasks, so a prerequisite is that they either handle InterruptedException in
     *  a sane way or they don't handle it all: if they ignore and squash it, then this is not going to work. */
    public static void shutdownNowAndAwaitTermination(ScopedLogger logger, ExecutorService executorService,
        String serviceName)
    {
        logger.withScopedInfo("Shutting down executor service {}", serviceName).run(() -> {
            executorService.shutdownNow();
            try
            {
                if (!executorService.awaitTermination(TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                {
                    logger.error("{} executor service did not terminate within {} seconds; stopping anyway",
                        serviceName,
                        TERMINATION_TIMEOUT_SECONDS);
                }
            }
            catch (InterruptedException e)
            {
                logger.error("Interrupted while waiting for " + serviceName + " executor service to terminate", e);
            }
        });
    }
}
