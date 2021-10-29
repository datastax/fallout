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
package com.datastax.fallout.ops.commands;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.EvictingQueue;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.HashedWheelTimer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.NamedThreadFactory;

/**
 * Represents the running state of a command started on a node
 *
 * @see com.datastax.fallout.ops.Node#execute(String)
 */
public abstract class NodeResponse
{
    private static final String CMD_FAIL_LOG_PREFIX = "[CMD-FAIL] ";

    private final Node owner;
    private final String command;
    public final Logger logger;
    private boolean asyncWaitStarted = false;
    private final List<Consumer<NodeResponse>> completionListeners = Collections.synchronizedList(new ArrayList<>());
    private boolean killed = false;

    /**
     * We need our own timer here, so that we can put NodeResponse.await inside another call using
     * Utils.asyncAwait, to avoid deadlocks.
     */
    private static final HashedWheelTimer timer = new HashedWheelTimer(new NamedThreadFactory("NodeResponse"));

    public NodeResponse(Node owner, String command)
    {
        this(owner, command, owner.logger());
    }

    public NodeResponse(Node owner, String command, Logger logger)
    {
        this.owner = owner;
        this.command = command;
        this.logger = logger;
    }

    /**
     * @return the node where this response will come from
     */
    public Node getOwner()
    {
        return owner;
    }

    /**
     * @return the command used to generate this response
     */
    public String getCommand()
    {
        return command;
    }

    /**
     *
     * Used internally by await functions.
     * We need to ensure the output is read in a
     * timely fashion to avoid blocking the buffer
     *
     * @return STDOUT of the process as a Stream
     * @throws IOException
     */
    protected abstract InputStream getOutputStream() throws IOException;

    /**
     *
     * Used internally by await functions.
     * We need to ensure the output is read in a
     * timely fashion to avoid blocking the buffer
     *
     * @return STDERR of the process as a Stream
     *
     * @throws IOException
     */
    protected abstract InputStream getErrorStream() throws IOException;

    /**
     *
     * @return Exitcode of the process (0 means success)
     */
    public abstract int getExitCode();

    /** Return whether the process exited due to a signal */
    @VisibleForTesting
    public boolean wasKilled()
    {
        return killed;
    }

    /**
     *
     * @return true if process ended, false means it's still running
     */
    public abstract boolean isCompleted();

    /**
     * Kills process if not complete
     */
    public final void kill()
    {
        doKill();
        killed = true;
    }

    protected abstract void doKill();

    public static class WaitOptions
    {
        private static final int DEFAULT_NO_OUTPUT_TIMEOUT_MINS = 13;
        public static final Duration DEFAULT_NO_OUTPUT_TIMEOUT = Duration.minutes(DEFAULT_NO_OUTPUT_TIMEOUT_MINS);
        private static final Duration DEFAULT_TIMEOUT_WITH_OUTPUT =
            Duration.minutes(3 * DEFAULT_NO_OUTPUT_TIMEOUT_MINS);
        public static final Duration DEFAULT_CHECK_INTERVAL = Duration.seconds(1L);

        /**
         * default will be determined by effectiveTimeouts()
         */
        public Duration timeout = null;
        public Optional<Duration> noOutputTimeout = Optional.of(DEFAULT_NO_OUTPUT_TIMEOUT);
        public Consumer<String> stdoutConsumer = stdoutLine -> {
        };
        public Consumer<String> stderrConsumer = stderrLine -> {
        };
        public Optional<Logger> logger = Optional.empty();
        public boolean outputLogging = true;
        public Duration checkInterval = DEFAULT_CHECK_INTERVAL;
        public IntPredicate exitCodeIsError = n -> n != 0;

        public void nonZeroIsNoError()
        {
            this.exitCodeIsError = n -> false;
        }

        public Optional<Logger> outputLogger(NodeResponse response)
        {
            return outputLogging ? Optional.of(logger(response)) : Optional.empty();
        }

        public Logger logger(NodeResponse response)
        {
            return this.logger.orElse(response.logger);
        }

        public Pair<Duration, Optional<Duration>> effectiveTimeouts()
        {
            // higher default hard timeout if we have a no output timeout
            final Duration defaultHardTimeout = this.noOutputTimeout.isPresent() ?
                DEFAULT_TIMEOUT_WITH_OUTPUT :
                Utils.DEFAULT_TIMEOUT;

            // we intentionally use == instead of equals below
            final Duration hardTimeout = this.timeout == null || this.timeout == Utils.DEFAULT_TIMEOUT ?
                defaultHardTimeout :
                this.timeout;

            // only keep no output timeout when it is smaller than hard timeout
            final Optional<Duration> noOutputTimeout = this.noOutputTimeout
                .filter(noOutputTimeout_ -> noOutputTimeout_.toNanos() < hardTimeout.toNanos());

            return Pair.of(hardTimeout, noOutputTimeout);
        }

        public void applyTo(WaitOptions other)
        {
            other.timeout = timeout;
            other.noOutputTimeout = noOutputTimeout;
            other.checkInterval = checkInterval;
            other.stdoutConsumer = stdoutConsumer;
            other.stderrConsumer = stderrConsumer;
            other.logger = logger;
            other.outputLogging = outputLogging;
            other.exitCodeIsError = exitCodeIsError;
        }
    }

    public NodeResponseWait doWait()
    {
        return new NodeResponseWait(this, createWaitOptions());
    }

    /**
     * @see Utils#waitForSuccess(Logger, Collection, WaitOptionsAdjuster)
     */
    public boolean waitForSuccess()
    {
        return doWait().forSuccess();
    }

    /**
     * @see Utils#waitForSuccess(Logger, Collection, WaitOptionsAdjuster)
     */
    public boolean waitForSuccess(Duration timeout)
    {
        return doWait().withTimeout(timeout).forSuccess();
    }

    /**
     * @see Utils#waitForQuietSuccess(Logger, Collection, WaitOptionsAdjuster)
     */
    public boolean waitForQuietSuccess()
    {
        return doWait()
            .withDisabledTimeoutAfterNoOutput()
            .forSuccess();
    }

    /**
     * @see Utils#waitForQuietSuccess(Logger, Collection, WaitOptionsAdjuster)
     */
    public boolean waitForQuietSuccess(Duration timeout)
    {
        return doWait()
            .withDisabledTimeoutAfterNoOutput()
            .withTimeout(timeout)
            .forSuccess();
    }

    /**
     * non-zero exit codes are not logged as errors
     */
    public boolean waitForOptionalSuccess()
    {
        return doWait().withNonZeroIsNoError().forSuccess();
    }

    public static class NodeResponseWait
    {
        private final NodeResponse response;
        private final WaitOptions waitOptions;

        public NodeResponseWait(NodeResponse response, WaitOptions waitOptions)
        {
            this.response = response;
            this.waitOptions = waitOptions;
        }

        public NodeResponseWait withTimeout(Duration timeout)
        {
            this.waitOptions.timeout = timeout;
            return this;
        }

        public NodeResponseWait withTimeoutAfterNoOutput(Duration noOutputTimeout)
        {
            this.waitOptions.noOutputTimeout = Optional.of(noOutputTimeout);
            return this;
        }

        public NodeResponseWait withDisabledTimeoutAfterNoOutput()
        {
            this.waitOptions.noOutputTimeout = Optional.empty();
            return this;
        }

        public NodeResponseWait withCheckInterval(Duration checkInterval)
        {
            this.waitOptions.checkInterval = checkInterval;
            return this;
        }

        public NodeResponseWait withStdoutConsumer(Consumer<String> stdoutConsumer)
        {
            this.waitOptions.stdoutConsumer = stdoutConsumer;
            return this;
        }

        public NodeResponseWait withStderrConsumer(Consumer<String> stderrConsumer)
        {
            this.waitOptions.stderrConsumer = stderrConsumer;
            return this;
        }

        public NodeResponseWait withOutputConsumer(Consumer<String> outputConsumer)
        {
            return this.withStdoutConsumer(outputConsumer).withStderrConsumer(outputConsumer);
        }

        /**
         * whether stdout/stderr of the running process should be logged
         */
        public NodeResponseWait withDisabledOutputLogging()
        {
            this.waitOptions.outputLogging = false;
            return this;
        }

        public NodeResponseWait withLogger(Logger logger)
        {
            this.waitOptions.logger = Optional.of(logger);
            return this;
        }

        /**
         * whether non-zero exit codes should be logged as errors
         */
        public NodeResponseWait withNonZeroIsNoError()
        {
            this.waitOptions.nonZeroIsNoError();
            return this;
        }

        /**
         * predicate to determine which exit codes should be logged as errors
         */
        public NodeResponseWait withExitCodeIsError(IntPredicate exitCodeIsError)
        {
            this.waitOptions.exitCodeIsError = exitCodeIsError;
            return this;
        }

        public CompletableFuture<Boolean> forProcessEndAsync()
        {
            return this.response.awaitAsync(this.waitOptions::applyTo);
        }

        public CompletableFuture<Optional<Integer>> forExitCodeAsync()
        {
            return forProcessEndAsync().thenApplyAsync(noTimeout -> noTimeout ?
                Optional.of(response.getExitCode()) :
                Optional.empty());
        }

        public CompletableFuture<Boolean> forSuccessAsync()
        {
            return forProcessEndAsync().thenApplyAsync(noTimeout -> noTimeout && response.getExitCode() == 0);
        }

        public boolean forProcessEnd()
        {
            return forProcessEndAsync().join();
        }

        public Optional<Integer> forExitCode()
        {
            return forExitCodeAsync().join();
        }

        public boolean forSuccess()
        {
            return forSuccessAsync().join();
        }
    }

    public interface WaitOptionsAdjuster
    {
        void adjust(WaitOptions wo);
    }

    protected WaitOptions createWaitOptions()
    {
        WaitOptions res = new WaitOptions();
        return res;
    }

    public CompletableFuture<Boolean> awaitAsync()
    {
        return awaitAsync(wo -> {});
    }

    public void addCompletionListener(Consumer<NodeResponse> completionListener)
    {
        if (asyncWaitStarted)
        {
            String logMsg = "addCompletionListener on already started NodeResponse: " + command;
            logger.warn(logMsg);
        }
        completionListeners.add(completionListener);
    }

    private static final Executor lineSupplierExecutor =
        Executors.newCachedThreadPool(new NamedThreadFactory("CmdOut"));

    /**
     * An asynchronous wait.
     *
     * The future will complete when isCompleted() returns true
     *
     * Many processes can be waited on via one shared worker thread
     *
     * @see Utils#awaitConditionAsync
     */
    public CompletableFuture<Boolean> awaitAsync(WaitOptionsAdjuster adjuster)
    {
        WaitOptions waitOptions = createWaitOptions();
        adjuster.adjust(waitOptions);
        final Logger logger = waitOptions.logger(this);
        final Pair<Duration, Optional<Duration>> timeouts = waitOptions.effectiveTimeouts();
        final Duration timeout = timeouts.getLeft();
        final Optional<Duration> noOutputTimeout = timeouts.getRight();

        String additionalInfo = noOutputTimeout.map(t -> "no-output-timeout: " + t.toHM() + ", ").orElse("");
        logger.info("Waiting for command ({}hard-timeout: {}): {}", additionalInfo, timeout.toHM(), command);

        if (asyncWaitStarted)
        {
            String logMsg = "awaitAsync on already awaited NodeResponse: " + command;
            logger.warn(logMsg);
        }

        asyncWaitStarted = true;

        class Context
        {
            static final int MAX_LAST_LINES = 20;
            final BufferedReader stream;
            final String type;
            final Consumer<String> consumer;
            final EvictingQueue<String> lastLines = EvictingQueue.create(MAX_LAST_LINES);
            CompletableFuture<Boolean> lineSupplier;

            Context(BufferedReader stream, Consumer<String> consumer, String type)
            {
                this.stream = stream;
                this.consumer = consumer;
                this.type = type;
            }

            void handleLine(String line)
            {
                lastLines.add(line);
                consumer.accept(line);
            }

            public String appendLastLines(String logMsg)
            {
                String res = logMsg;
                if (!lastLines.isEmpty())
                {
                    res += "\n" + type + " (last " + lastLines.size() + " lines):\n";
                    for (String line : lastLines)
                    {
                        res += line + "\n";
                    }
                }
                return res;
            }
        }

        final Context[] streams;
        try
        {
            streams = new Context[] {
                new Context(
                    new BufferedReader(new InputStreamReader(getOutputStream(), StandardCharsets.UTF_8)),
                    waitOptions.stdoutConsumer,
                    "STDOUT"),
                new Context(
                    new BufferedReader(new InputStreamReader(getErrorStream(), StandardCharsets.UTF_8)),
                    waitOptions.stderrConsumer,
                    "STDERR")
            };
        }
        catch (IOException e)
        {
            logger.error("Error waiting for node response", e);
            return CompletableFuture.completedFuture(false);
        }

        AtomicLong lastOutputLineTime = new AtomicLong(0);
        for (Context ctx : streams)
        {
            ctx.lineSupplier = CompletableFuture.supplyAsync(
                () -> {
                    try
                    {
                        String line;
                        while ((line = ctx.stream.readLine()) != null)
                        {
                            String trimmedLine = line.trim();
                            if (!trimmedLine.isEmpty() && !trimmedLine.equals("\n"))
                            {
                                final Optional<Logger> outputLogger = waitOptions.outputLogger(this);
                                if (outputLogger.isPresent())
                                {
                                    outputLogger.get().info("{}: {}", ctx.type, line);
                                }
                                ctx.handleLine(line);
                            }
                            lastOutputLineTime.set(System.nanoTime());
                        }
                        return true;
                    }
                    catch (Exception e)
                    {
                        logger.error("Error waiting for node response output", e);
                        return false;
                    }
                }, lineSupplierExecutor);
        }

        AtomicBoolean wasNoOutputTimeout = new AtomicBoolean(false);
        List<Utils.TimeoutCheck> timeoutChecks = List.of();
        if (noOutputTimeout.isPresent())
        {
            long outputTimeoutNano = noOutputTimeout.get().toNanos();
            timeoutChecks = new ArrayList<>(1);
            timeoutChecks.add(nanoTime -> {
                long lastOutputNano = lastOutputLineTime.get();
                if (lastOutputNano > 0) // we only timeout processes that have had output before
                {
                    if (nanoTime >= lastOutputNano + outputTimeoutNano)
                    {
                        wasNoOutputTimeout.set(true);
                        return true;
                    }
                }
                return false;
            });
        }

        Utils.AwaitConditionOptions awaitOptions = new Utils.AwaitConditionOptions(logger, this::isCompleted, timeout,
            timer, waitOptions.checkInterval);
        awaitOptions.addTimeoutChecks(timeoutChecks);
        return Utils.awaitConditionAsync(awaitOptions).thenApplyAsync(
            completedWithoutTimeout -> {
                completionListeners.forEach(completionListener -> completionListener.accept(this));
                completionListeners.clear();
                String nodeInfo = owner == null ? "" : " on node '" + owner.getId() + "'";

                // Finish processing streams _before_ we log command completion, otherwise we'll miss
                // command output after the command has been logged as completed.
                if (completedWithoutTimeout && !wasKilled())
                {
                    // Wait for stream processing to complete
                    for (Context ctx : streams)
                    {
                        try
                        {
                            Uninterruptibles.getUninterruptibly(ctx.lineSupplier, 1, TimeUnit.MINUTES);
                        }
                        catch (ExecutionException e)
                        {
                            throw new CompletionException(e);
                        }
                        catch (TimeoutException e)
                        {
                            logger.error(
                                "Command{} timed out waiting for output streams to close; this may be due to a zombie process, see FAL-1119",
                                nodeInfo);
                        }
                    }
                }
                else
                {
                    // Stop processing output since we will either kill this process due to timeout, or have already
                    // killed it.  For killed processes, this is a hack to get around children of the target process
                    // keeping the stream open; once FAL-1119 is done, it should no longer be necessary.
                    logger.debug("Command{} timed out or killed: cancelling output streams listeners", nodeInfo);
                    for (Context ctx : streams)
                    {
                        if (!ctx.lineSupplier.isDone())
                        {
                            ctx.lineSupplier.cancel(true);
                        }
                    }
                }

                if (completedWithoutTimeout)
                {
                    int exitCode = getExitCode();
                    if (exitCode == 0)
                    {
                        logger.info("Command{} completed with exit code {}: {}", nodeInfo, exitCode, command);
                    }
                    else
                    {
                        String logMsg = "Command{} completed with non-zero exit code {}: {}";
                        if (waitOptions.exitCodeIsError.test(exitCode))
                        {
                            String rawErrorMsg = CMD_FAIL_LOG_PREFIX + logMsg;
                            String errorMsg = maybeAppendCommandOutputToLogMessage(rawErrorMsg);
                            boolean didNotAppendOutput = errorMsg.length() == rawErrorMsg.length();
                            if (didNotAppendOutput)
                            {
                                for (Context ctx : streams)
                                {
                                    errorMsg = ctx.appendLastLines(errorMsg);
                                }
                            }
                            logger.error(errorMsg, nodeInfo, exitCode, command);
                        }
                        else
                        {
                            logger.info(logMsg, nodeInfo, exitCode, command);
                        }
                    }
                }
                else
                {
                    if (wasNoOutputTimeout.get())
                    {
                        String errorMsg = CMD_FAIL_LOG_PREFIX + "Command{} timed out due to no output after {}: {}";
                        logger.error(errorMsg, nodeInfo, waitOptions.noOutputTimeout.get(), command);
                    }
                    else
                    {
                        String errorMsg = CMD_FAIL_LOG_PREFIX + "Command{} timed out after {}: {}";
                        logger.error(errorMsg, nodeInfo, timeout, command);
                    }

                    this.kill();
                }

                return completedWithoutTimeout;
            });
    }

    protected String maybeAppendCommandOutputToLogMessage(String logMsg)
    {
        return logMsg;
    }

    public FullyBufferedNodeResponse buffered()
    {
        Preconditions.checkState(!asyncWaitStarted, "Cannot buffer output retrospectively");
        return new FullyBufferedNodeResponse(this);
    }

    /**
     * Represents an Error state.
     */
    public static class ErrorResponse extends NodeResponse
    {
        public ErrorResponse(Node owner, String error)
        {
            super(owner, error);
        }

        public ErrorResponse(Logger logger, String error)
        {
            super(null, error, logger);
        }

        @Override
        protected InputStream getOutputStream() throws IOException
        {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        protected InputStream getErrorStream() throws IOException
        {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public int getExitCode()
        {
            return -1;
        }

        public boolean isCompleted()
        {
            return true;
        }

        @Override
        public void doKill()
        {
        }
    }

    private static class FutureNodeResponse extends NodeResponse
    {
        private final CompletableFuture<NodeResponse> future;

        private FutureNodeResponse(Node node, String command, Supplier<NodeResponse> supplier)
        {
            super(node, command);
            future = CompletableFuture.supplyAsync(supplier);
        }

        @Override
        protected InputStream getOutputStream() throws IOException
        {
            return future.join().getOutputStream();
        }

        @Override
        protected InputStream getErrorStream() throws IOException
        {
            return future.join().getErrorStream();
        }

        @Override
        public int getExitCode()
        {
            return future.join().getExitCode();
        }

        @Override
        public boolean isCompleted()
        {
            return future.isDone() && future.join().isCompleted();
        }

        @Override
        public void doKill()
        {
            future.join().kill();
        }

        @Override
        public CompletableFuture<Boolean> awaitAsync(WaitOptionsAdjuster adjuster)
        {
            return future.thenComposeAsync(nodeResponse -> nodeResponse.awaitAsync(adjuster));
        }
    }

    public static NodeResponse supplyAsync(Node node, String command, Supplier<NodeResponse> supplier)
    {
        return new FutureNodeResponse(node, command, supplier);
    }
}
