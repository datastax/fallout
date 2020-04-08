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
package com.datastax.fallout.ops;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.netty.util.HashedWheelTimer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ShellUtils;

/**
 * Yup. we have one.
 */
public class Utils
{
    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static final Duration DEFAULT_TIMEOUT = Duration.minutes(5);

    public interface TimeoutCheck
    {
        boolean isTimeout(long nanoTime);
    }

    /**
     * The condition will be evaluated repeatedly at the given interval until it returns true.
     * If an exception is thrown or the condition does not evaluate to true within the given timeout, false will be returned.
     * <p>
     * The default interval is 1 second.
     */
    public static class AwaitConditionOptions
    {
        final Logger logger;
        final Supplier<Boolean> condition;
        final Duration timeout;
        final HashedWheelTimer timer;
        final Duration interval;
        final List<TimeoutCheck> additionalTimeoutChecks = new ArrayList<>();

        public AwaitConditionOptions(Logger logger, Supplier<Boolean> condition, Duration timeout,
            HashedWheelTimer timer, Duration interval)
        {
            this.logger = logger;
            this.condition = condition;
            this.timeout = timeout;
            this.timer = timer;
            this.interval = interval;
        }

        public void addTimeoutChecks(List<TimeoutCheck> timeoutChecks)
        {
            additionalTimeoutChecks.addAll(timeoutChecks);
        }

        public void addTestRunAbortTimeout(Node node)
        {
            addTestRunAbortTimeout(node.getNodeGroup(), node.logger());
        }

        private void addTestRunAbortTimeout(NodeGroup nodeGroup, Logger logger)
        {
            additionalTimeoutChecks.add(nanoTime -> {
                // only abort waiting when we can interrupt, since this general wait helper could be used by teardown logic
                boolean testRunAborted = nodeGroup.currentOperationShouldBeAborted();
                if (testRunAborted)
                {
                    logger.warn("AwaitCondition timed out early due to aborted TestRun");
                }
                return testRunAborted;
            }
            );
        }
    }

    /**
     * Low grain polling of an async process (min 1 second)
     * useful for IO futures vs blocking
     *
     * @see AwaitConditionOptions
     */
    public static CompletableFuture<Boolean> awaitConditionAsync(AwaitConditionOptions awaitOptions)
    {
        long startNs = System.nanoTime();
        long timeoutNs = awaitOptions.timeout.toNanos();

        List<TimeoutCheck> timeoutChecks = new ArrayList<>(awaitOptions.additionalTimeoutChecks.size() + 1);
        timeoutChecks.add(nanoTime -> nanoTime >= startNs + timeoutNs);
        timeoutChecks.addAll(awaitOptions.additionalTimeoutChecks);

        CompletableFuture<Boolean> awaitTask = new CompletableFuture<>();
        loopUntilConditionOrTimeout(awaitTask, awaitOptions, timeoutChecks);
        return awaitTask;
    }

    private static void loopUntilConditionOrTimeout(CompletableFuture<Boolean> notifier,
        AwaitConditionOptions awaitOptions, List<TimeoutCheck> timeoutChecks)
    {
        Preconditions.checkArgument(!timeoutChecks.isEmpty(), "loopUntilConditionOrTimeout without timeout checks");
        awaitOptions.timer.newTimeout(timeout -> {
            Optional<Boolean> finalResult =
                checkConditionOrTimeout(awaitOptions.logger, awaitOptions.condition, timeoutChecks);
            if (finalResult.isPresent())
            {
                notifier.complete(finalResult.get());
                return;
            }
            else
            {
                // We have time. wait and try again
                loopUntilConditionOrTimeout(notifier, awaitOptions, timeoutChecks);
            }
        }, awaitOptions.interval.value, awaitOptions.interval.unit);
    }

    private static Optional<Boolean> checkConditionOrTimeout(Logger logger, Supplier<Boolean> condition,
        List<TimeoutCheck> timeoutChecks)
    {
        String lastCheck = "condition";
        try
        {
            if (condition.get())
            {
                return Optional.of(true);
            }
            lastCheck = "timeout";
            long nowNs = System.nanoTime();
            for (TimeoutCheck timeoutCheck : timeoutChecks)
            {
                if (timeoutCheck.isTimeout(nowNs))
                {
                    return Optional.of(false);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error("Failed to check " + lastCheck + ", will abort awaitConditionAsync", t);
            return Optional.of(false);
        }
        return Optional.empty();
    }

    /**
     * Helper to wait for many processes to complete using a single thread
     *
     * @return if the wait succeeded or if it timedout / encountered errors
     */
    public static <T extends NodeResponse> boolean waitForProcessEnd(
        Logger logger, Collection<T> nodeResponses, NodeResponse.WaitOptionsAdjuster adjuster)
    {
        return waitForAll(
            nodeResponses.stream().map(nr -> nr.awaitAsync(adjuster)).collect(Collectors.toList()), logger,
            "waitForProcessEnd");
    }

    /**
     * @see Utils#waitForSuccess(Logger, Collection, NodeResponse.WaitOptionsAdjuster)
     */
    public static <T extends NodeResponse> boolean waitForSuccess(Logger logger, Collection<T> nodeResponses)
    {
        return waitForSuccess(logger, nodeResponses, wo -> {});
    }

    /**
     * @see Utils#waitForSuccess(Logger, Collection, NodeResponse.WaitOptionsAdjuster)
     */
    public static <T extends NodeResponse> boolean waitForSuccess(
        Logger logger, Collection<T> nodeResponses, Duration timeout)
    {
        return waitForSuccess(logger, nodeResponses, wo -> wo.timeout = timeout);
    }

    /**
     * Like waitForSuccess but the waiting is done with the (default) no-output timeout disabled.
     *
     * You need to use this way of waiting when you know the command of the responses will output something initially,
     * but then not output anything for a longer time.
     *
     * @see Utils#waitForSuccess(Logger, Collection, NodeResponse.WaitOptionsAdjuster)
     */
    public static <T extends NodeResponse> boolean waitForQuietSuccess(
        Logger logger, Collection<T> nodeResponses, NodeResponse.WaitOptionsAdjuster adjuster)
    {
        return waitForSuccess(logger, nodeResponses, wo -> {
            adjuster.adjust(wo);
            wo.noOutputTimeout = Optional.empty();
        });
    }

    /**
     * Helper to wait for many processes to complete using a single thread.
     *
     * @param nodeResponses the responses to wait on
     * @return Returns true if the wait succeeded without timeout and all responses had exit code 0.
     * Returns false if it encountered any timeouts / errors / non-zero exit codes.
     */
    public static <T extends NodeResponse> boolean waitForSuccess(
        Logger logger, Collection<T> nodeResponses, NodeResponse.WaitOptionsAdjuster adjuster)
    {
        boolean allFinishedWithoutTimeout = waitForProcessEnd(logger, nodeResponses, adjuster);
        return allFinishedWithoutTimeout && nodeResponses.stream().allMatch(f -> f.getExitCode() == 0);
    }

    /** Return a future that will complete when all futures are complete, and contains whether their
     *  results were all true. Will propagate CancellationException and CompletionException. */
    public static CompletableFuture<Boolean> waitForAllAsync(Collection<CompletableFuture<Boolean>> futures)
    {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}))
            .thenApplyAsync(v -> futures.stream().allMatch(CompletableFuture::join));
    }

    /** Call waitForAllAsync(futures); if an exception occurs, log
     *  it to logger using operationName and set a false result. */
    public static CompletableFuture<Boolean> waitForAllAsync(
        Collection<CompletableFuture<Boolean>> futures, Logger logger,
        String operationName)
    {
        return waitForAllAsync(futures)
            .exceptionally(e -> {
                logger.error("Error during " + operationName, e);
                return false;
            });
    }

    public static boolean waitForAll(Collection<CompletableFuture<Boolean>> futures, Logger logger,
        String operationName)
    {
        return waitForAllAsync(futures, logger, operationName).join();
    }

    public static boolean waitForAll(Collection<CompletableFuture<Boolean>> futures)
    {
        return waitForAllAsync(futures).join();
    }

    public static long parseLong(String value)
    {
        long multiplier = 1;
        value = value.trim().toLowerCase();
        switch (value.charAt(value.length() - 1))
        {
            case 'b':
                multiplier *= 1000;
            case 'm':
                multiplier *= 1000;
            case 'k':
                multiplier *= 1000;
                value = value.substring(0, value.length() - 1);
        }
        return Long.parseLong(value) * multiplier;
    }

    /**
     * Parses the only value from a JSON singleton map.
     * Example -k commitlog_directory output:
     *
     *  {
     *    "0": "/var/lib/cassandra/data"
     *  }
     *
     * Returns "/var/lib/cassandra/data"
     *
     * Example -k data_file_directories:
     *
     *  {
     *    "0": [
     *      "/var/lib/cassandra/data",
     *      "/mnt/cass_data_disks/data1"
     *    ]
     *  }
     *
     * Returns ["/var/lib/cassandra/data", "/mnt/cass_data_disks/data1"]
     */
    public static <T> T parseJsonSingletonMap(String json)
    {
        try
        {
            Map<String, T> output = (Map<String, T>) new ObjectMapper().readValue(json, Object.class);

            if (output.size() != 1)
            {
                throw new IllegalArgumentException(String.format(
                    "toParse is only supposed to contain output fom one node but found:\n%s", json));
            }

            return output.values().iterator().next();
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    public static Optional<byte[]> getResource(Object context, String resourceName)
    {
        Class clazz = context.getClass();
        InputStream is = clazz.getClassLoader().getResourceAsStream(resourceName);

        //Try with package namespace
        if (is == null)
        {
            String p = clazz.getPackage().getName();
            String prefix = String.join(File.separator, StringUtils.split(p, "."));
            is = clazz.getClassLoader()
                .getResourceAsStream(String.format("%s%s%s", prefix, File.separator, resourceName));
        }

        if (is != null)
        {
            try
            {
                ByteArrayOutputStream bao = new ByteArrayOutputStream();

                while (is.available() > 0)
                    bao.write(is.read());

                return Optional.of(bao.toByteArray());
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        return Optional.empty();
    }

    public static void writeStringToFile(File file, String content)
    {
        Exceptions.runUncheckedIO(() -> Files.write(file.toPath(), content.getBytes(StandardCharsets.UTF_8)));
    }

    public static String readStringFromFile(File file)
    {
        return Exceptions.getUncheckedIO(() -> FileUtils.readFileToString(file, Charset.forName("utf-8")));
    }

    private static ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    public static <T> T fromJson(String json, Class<T> clazz)
    {
        return Exceptions.getUncheckedIO(() -> jsonMapper.readValue(json, clazz));
    }

    public static Map<String, String> fromJsonMap(String json)
    {
        return Exceptions.getUncheckedIO(() -> jsonMapper.readValue(json, Map.class));
    }

    public static List<String> fromJsonList(String json)
    {
        return Exceptions.getUncheckedIO(() -> jsonMapper.readValue(json, List.class));
    }

    public static String json(Object object)
    {
        return Exceptions.getUncheckedIO(() -> jsonMapper.writeValueAsString(object));
    }

    public static JsonNode getJsonNode(String json, String pathToNode)
    {
        JsonNode node = Exceptions.getUncheckedIO(() -> jsonMapper.readTree(json));
        return node.at(pathToNode);
    }

    public static <T> List<T> pickInRandomOrder(List<T> candidates, int count, Random r)
    {
        if (candidates.isEmpty())
        {
            return Collections.emptyList();
        }
        if (count <= 0)
        {
            throw new IllegalArgumentException("pickRandomly got invalid count: " + count);
        }
        List<T> tmp = new ArrayList<>(candidates);
        Collections.shuffle(tmp, r);
        return tmp.subList(0, Math.min(count, candidates.size()));
    }

    public static List<String> wrapCommandWithBash(String command, boolean remoteCommand)
    {
        List<String> fullCmd = new ArrayList<>();
        fullCmd.add("/bin/bash");
        fullCmd.add("-o");
        fullCmd.add("pipefail"); // pipe returns first non-zero exit code
        if (remoteCommand)
        {
            // Remote commands should be run in a login shell, since they'll need the environment
            // to be set up correctly.  Local commands should already be in this situation,
            // since fallout should have been run with the correct environment already in place.
            fullCmd.add("-l");
        }
        fullCmd.add("-c"); // execute following command
        if (remoteCommand)
        {
            // Remote commands need to be quoted again, to prevent expansion as they're passed to ssh.
            String escapedCmd = ShellUtils.escape(command, true);
            fullCmd.add(escapedCmd);
        }
        else
        {
            fullCmd.add(command);
        }
        return fullCmd;
    }

    public static <T extends PropertyBasedComponent> List<T> loadComponents(Class<T> componentClass)
    {
        try
        {
            ServiceLoader<T> loadedComponents = ServiceLoader.load(componentClass);
            return Lists.newArrayList(loadedComponents);
        }
        catch (Throwable t)
        {
            log.error("Failed to loadComponents for " + componentClass, t);
            throw t;
        }
    }
}
