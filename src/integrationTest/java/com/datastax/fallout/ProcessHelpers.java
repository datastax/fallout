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
package com.datastax.fallout;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class ProcessHelpers
{
    private static final Logger logger = LoggerFactory.getLogger(ProcessHelpers.class);

    // Blocking methods should not run on the ForkJoinPool
    private static final Executor outputExecutor = Executors.newCachedThreadPool();

    private static void assertCompleted(java.lang.Process process, Duration timeout,
        List<CompletableFuture<Void>> outputLoggers)
    {
        boolean completed = Exceptions
            .getUninterruptibly(() -> process.waitFor(timeout.getSeconds(), TimeUnit.SECONDS));
        if (!completed)
        {
            process.destroyForcibly();
        }

        outputLoggers.forEach(outputLogger -> Exceptions.getUnchecked(
            () -> outputLogger.get(5, TimeUnit.SECONDS)));

        assertThat(completed).isTrue();
    }

    private static CompletableFuture<Void> logCommandOutput(InputStream inputStream, String streamName)
    {
        return CompletableFuture.runAsync(() -> {
            final var stream = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String line;
            while ((line = Exceptions.getUncheckedIO(stream::readLine)) != null)
            {
                logger.info("{}: {}", streamName, line.trim());
            }
        }, outputExecutor);
    }

    public static int run(List<String> command, Map<String, String> extraEnv, Duration timeout)
    {
        ProcessBuilder processBuilder = new ProcessBuilder();

        logger.info("proc run: {}\nwith env: {}", command, extraEnv);

        processBuilder
            .command(command)
            .environment().putAll(extraEnv);

        java.lang.Process process = Exceptions.getUncheckedIO(processBuilder::start);
        final var outputLoggers = List.of(
            logCommandOutput(process.getInputStream(), "STDOUT"),
            logCommandOutput(process.getErrorStream(), "STDERR"));

        assertCompleted(process, timeout, outputLoggers);
        int exitCode = process.exitValue();
        logger.info("proc exit: {} {}", exitCode, command);
        return exitCode;
    }
}
