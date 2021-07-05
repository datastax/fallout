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
package com.datastax.fallout.ops;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;

import com.datastax.fallout.components.common.provider.NodeInfoProvider;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ShellUtils;

import static java.util.stream.Collectors.joining;

/**
 * Yup. we have one.
 */
public class Utils
{
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
            HashedWheelTimer timer)
        {
            this(logger, condition, timeout, timer, Duration.seconds(1L));
        }

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

        public void addTestRunAbortTimeout(NodeGroup nodeGroup)
        {
            addTestRunAbortTimeout(nodeGroup, nodeGroup.logger());
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
     * @see Utils#waitForQuietSuccess(Logger, Collection, NodeResponse.WaitOptionsAdjuster)
     */
    public static <T extends NodeResponse> boolean waitForQuietSuccess(
        Logger logger, Collection<T> nodeResponses, Duration timeout)
    {
        return waitForQuietSuccess(logger, nodeResponses, wo -> {
            wo.timeout = timeout;
        });
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

    public static boolean waitForPort(String hostname, int port, Logger logger)
    {
        return waitForPort(hostname, port, DEFAULT_TIMEOUT, logger, false);
    }

    public static boolean waitForPort(String hostname, int port, Duration timeout, Logger logger)
    {
        return waitForPort(hostname, port, timeout, logger, false);
    }

    /**
     * Wait for a port to be open until a deadline is reached (+/- 1 second)
     */
    public static boolean waitForPort(String hostname, int port, Duration timeout, Logger logger, boolean quiet)
    {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (System.nanoTime() < deadlineNanos)
        {
            SocketChannel channel = null;

            try
            {
                logger.info("Checking {}:{}", hostname, port);
                channel = SocketChannel.open(new InetSocketAddress(hostname, port));
            }
            catch (IOException e)
            {
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }

            if (channel != null)
            {
                try
                {
                    channel.close();
                }
                catch (IOException e)
                {
                    //Close quietly
                }

                logger.info("Connected to {}:{}", hostname, port);
                return true;
            }
        }

        //The port never opened
        if (!quiet)
        {
            logger.warn("Failed to connect to {}:{} after {} sec", hostname, port, timeout.toSeconds());
        }

        return false;
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

    public static long parseMemory(String value)
    {
        long multiplier = 1;
        value = value.trim().toLowerCase();
        switch (value.charAt(value.length() - 1))
        {
            case 'g':
                multiplier *= 1000;
            case 'm':
                multiplier *= 1000;
            case 'k':
                multiplier *= 1000;
            case 'b':
                value = value.substring(0, value.length() - 1);
        }

        return Long.parseLong(value) * multiplier;
    }

    /**
     * Takes an input stream consisting of String representations of paths separated by newlines
     * and returns a collection of String representations of those paths prepended with the provided prefix
     *
     * @param input Stream to use as input
     * @param prefix Prefix to prepend to strings
     * @return collection of Paths as strings
     */
    public static Collection<String> parsePaths(String input, String prefix) throws IOException
    {
        Collection<String> paths = new HashSet<>();

        try (BufferedReader outputReader = new BufferedReader(new StringReader(input)))
        {
            String line;
            while ((line = outputReader.readLine()) != null)
            {
                paths.add(Paths.get(prefix, line.trim()).toString());
            }
        }

        return paths;
    }

    private static Stream<String> getNodeInfo(List<Node> nodes, Function<NodeInfoProvider, String> fun)
    {
        return nodes.stream()
            .map(n -> n.getProvider(NodeInfoProvider.class))
            .map(fun);
    }

    public static Stream<String> getPublicNodeIps(List<Node> nodes)
    {
        return getNodeInfo(nodes, NodeInfoProvider::getPublicNetworkAddress);
    }

    public static Stream<String> getPrivateNodeIps(List<Node> nodes)
    {
        return getNodeInfo(nodes, NodeInfoProvider::getPrivateNetworkAddress);
    }

    public static String getPublicNodeIpsCommaSeparated(List<Node> nodes)
    {
        return getPublicNodeIps(nodes).collect(joining(","));
    }

    public static String getPrivateNodeIpsCommaSeparated(List<Node> nodes)
    {
        return getPrivateNodeIps(nodes).collect(joining(","));
    }

    /**
     * Determine whether or not two node groups should communicate via private IP addresses. When sharing a cloud
     * provider, node groups should communicate via Private IPs. Public Ips should be used when dealing with node groups
     * communicating over two different public clouds or when using a private client and public server
     */
    private static boolean usePrivateIps(NodeGroup client, NodeGroup server)
    {
        return client.getProvisioner().usePrivateIps(server.getProvisioner());
    }

    /**
     * @return the IP needed to connect to the server node
     */
    public static String getContactPoint(Node clientNode, Node serverNode)
    {
        return getContactPoints(List.of(clientNode), List.of(serverNode)).get(0);
    }

    public static List<String> getContactPoints(List<Node> clientNodes, List<Node> serverNodes)
    {
        return usePrivateIps(clientNodes.get(0).getNodeGroup(), serverNodes.get(0).getNodeGroup()) ?
            getPrivateNodeIps(serverNodes).collect(Collectors.toList()) : getPublicNodeIps(serverNodes).collect(
                Collectors.toList());
    }

    public static String getContactPointsCommaSeparated(List<Node> clientNodes, List<Node> serverNodes)
    {
        return usePrivateIps(clientNodes.get(0).getNodeGroup(), serverNodes.get(0).getNodeGroup()) ?
            getPrivateNodeIpsCommaSeparated(serverNodes) : getPublicNodeIpsCommaSeparated(serverNodes);
    }

    public static <T> List<T> pickInRandomOrder(List<T> candidates, int count, Random r)
    {
        if (candidates.isEmpty())
        {
            return List.of();
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

    /**
     * Executes a command on the first node of a nodegroup and returns stdOut on success
     *
     * @param nodeGroup
     * @param cmd
     * @return
     */
    public static Optional<String> captureStdOutFromFirstNodeOnSuccess(NodeGroup nodeGroup, String cmd)
    {
        FullyBufferedNodeResponse r = nodeGroup.getNodes().get(0).executeBuffered(cmd);
        if (r.waitForOptionalSuccess())
        {
            return Optional.of(r.getStdout());
        }
        return Optional.empty();
    }

    /**
     * Returns the first N lines of a given string comma separated
     */
    public static String getNLines(String s, int n)
    {
        return new BufferedReader(new StringReader(s)).lines().limit(n).collect(Collectors.joining(", "));
    }

    public static List<String> toSortedList(Set<String> set)
    {
        return toSortedList(set, Function.identity());
    }

    public static <T> List<String> toSortedList(Set<T> set, Function<T, String> toStringFunc)
    {
        return set.stream().map(toStringFunc).sorted().collect(Collectors.toList());
    }
}
