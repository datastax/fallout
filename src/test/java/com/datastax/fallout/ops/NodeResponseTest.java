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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.LocalCommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeResponseTest
{
    private static final Logger logger = LoggerFactory.getLogger(NodeResponseTest.class);
    private static final CommandExecutor commandExecutor = new LocalCommandExecutor();

    private static FullyBufferedNodeResponse runAndWait(String cmd)
    {
        FullyBufferedNodeResponse res = run(cmd).buffered();
        res.doWait().forSuccess();
        return res;
    }

    private static NodeResponse run(String cmd)
    {
        return commandExecutor.executeLocally(logger, cmd);
    }

    @Test
    public void testOutputWatcherDeadlock()
    {
        char[] big = new char[Short.MAX_VALUE * 3];
        Arrays.fill(big, 'a');
        String bigString = String.valueOf(big);

        FullyBufferedNodeResponse r = runAndWait(String.format("echo %s", bigString));
        assertThat(r.getExitCode()).isEqualTo(0);
    }

    @Test
    public void testJoiningToASingleLine()
    {
        FullyBufferedNodeResponse r = runAndWait("echo -n foo && sleep 1 && echo -n bar");
        assertThat(r.getStdout()).isEqualTo("foobar");
    }

    @Test
    public void testJoiningToAMultiLine()
    {
        FullyBufferedNodeResponse r = runAndWait("echo foo && sleep 1 && echo bar");
        assertThat(r.getStdout()).isEqualTo("foo\nbar");
    }

    @Test
    public void testJoiningToAMultipleLines()
    {
        FullyBufferedNodeResponse r = runAndWait("echo 'foo\nba\n\n' && sleep 1 && echo -n r");
        assertThat(r.getStdout()).isEqualTo("foo\nba\nr");
    }

    private static void assertTimeout(Duration timeout, Duration noOutputTimeout)
    {
        StringBuilder stdout = new StringBuilder();
        NodeResponse r = run("echo 'foo' && sleep 5 && echo 'bar'");
        boolean success = r.doWait()
            .withTimeout(timeout)
            .withTimeoutAfterNoOutput(noOutputTimeout)
            .withStdoutConsumer(stdout::append)
            .forSuccess();
        assertThat(success).isFalse(); // timeout
        assertThat(stdout.toString()).isEqualTo("foo");

        Uninterruptibles.sleepUninterruptibly(6, TimeUnit.SECONDS);

        // process has been killed in the meantime http://tldp.org/LDP/abs/html/exitcodes.html
        int killExitCode = 128 + 9;
        assertThat(r.getExitCode()).isEqualTo(killExitCode);
        // but we still did not get the output after the timeout
        assertThat(stdout.toString()).isEqualTo("foo");
    }

    @Test
    public void testTimeout()
    {
        assertTimeout(Duration.seconds(2), Duration.minutes(10));
    }

    @Test
    public void testNoOutputTimeout()
    {
        assertTimeout(Duration.minutes(10), Duration.seconds(2));
    }

    @Test
    public void testStdOutAndErr()
    {
        FullyBufferedNodeResponse r = runAndWait("echo -n '111' && >&2 echo -n '222' && >&2 echo '333' && echo '444'");
        assertThat(r.getStdout()).isEqualTo("111444");
        assertThat(r.getStderr()).isEqualTo("222333");
    }

    private void assertNodeResponseLogsCorrectly(boolean withDisableOutputLogging) throws Exception
    {
        LoggerContext loggerContext = new LoggerContext();

        try (AutoCloseable closeable = loggerContext::stop)
        {
            loggerContext.start();
            ch.qos.logback.classic.Logger logger = loggerContext.getLogger("test");
            logger.detachAndStopAllAppenders();

            ListAppender<ILoggingEvent> listAppender = new ListAppender<ILoggingEvent>();
            listAppender.setContext(loggerContext);
            listAppender.start();
            logger.addAppender(listAppender);

            NodeResponse r = commandExecutor.executeLocally(NodeResponseTest.logger, "echo hello");

            NodeResponse.NodeResponseWait nodeResponseWait = r.doWait().withLogger(logger);
            if (withDisableOutputLogging)
            {
                nodeResponseWait = nodeResponseWait.withDisabledOutputLogging();
            }

            assertThat(nodeResponseWait.forSuccess()).isTrue();

            assertThat(listAppender.list)
                .anySatisfy(event -> assertThat(event.getFormattedMessage()).contains("Waiting for command"));
            assertThat(listAppender.list)
                .anySatisfy(event -> assertThat(event.getFormattedMessage()).contains("completed with exit code 0"));

            if (withDisableOutputLogging)
            {
                assertThat(listAppender.list)
                    .allSatisfy(event -> assertThat(event.getFormattedMessage()).doesNotContain("STDOUT"));
            }
            else
            {
                assertThat(listAppender.list)
                    .anySatisfy(event -> assertThat(event.getFormattedMessage()).contains("STDOUT"));
            }
        }
    }

    @Test
    public void testLogging() throws Exception
    {
        assertNodeResponseLogsCorrectly(false);
        assertNodeResponseLogsCorrectly(true);
    }

    @Test
    public void testEffectiveTimeouts()
    {
        NodeResponse.WaitOptions wo;

        wo = new NodeResponse.WaitOptions();
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(39), Optional.of(Duration.minutes(13))));

        wo = new NodeResponse.WaitOptions();
        wo.timeout = Duration.minutes(10);
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(10), Optional.empty()));

        wo = new NodeResponse.WaitOptions();
        wo.timeout = Duration.minutes(20);
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(20), Optional.of(Duration.minutes(13))));

        wo = new NodeResponse.WaitOptions();
        wo.noOutputTimeout = Optional.of(Duration.minutes(20));
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(39), Optional.of(Duration.minutes(20))));

        wo = new NodeResponse.WaitOptions();
        wo.noOutputTimeout = Optional.of(Duration.minutes(3));
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(39), Optional.of(Duration.minutes(3))));

        wo = new NodeResponse.WaitOptions();
        wo.noOutputTimeout = Optional.of(Duration.minutes(40));
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(39), Optional.empty()));

        wo = new NodeResponse.WaitOptions();
        wo.noOutputTimeout = Optional.empty();
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(5), Optional.empty()));

        wo = new NodeResponse.WaitOptions();
        wo.timeout = Duration.minutes(3);
        wo.noOutputTimeout = Optional.of(Duration.minutes(3));
        assertThat(wo.effectiveTimeouts()).isEqualTo(Pair.of(Duration.minutes(3), Optional.empty()));
    }

    public static class StressTests
    {
        private List<FakeNodeResponse> nodeResponses = List.of();

        private static class ExceptionCatchingThread extends Thread
        {
            private final String id;
            private volatile Optional<Throwable> caught = Optional.empty();
            private volatile boolean started = false;
            private final Runnable runnable;

            public ExceptionCatchingThread(String id, Runnable runnable)
            {
                this.id = id;
                this.runnable = runnable;

                setUncaughtExceptionHandler((ignored, ex) -> caught = Optional.of(ex));
                setName("ECT:" + id);

                start();
                while (!started)
                {
                    Thread.onSpinWait();
                }
            }

            @Override
            public void run()
            {
                started = true;
                runnable.run();
            }

            public void didNotThrow()
            {
                Exceptions.runUnchecked(this::join);
                assertThat(caught)
                    .withFailMessage("%s threw", id)
                    .isEmpty();
            }
        }

        static class FakeNodeResponse extends NodeResponse
        {
            private final PipedInputStream outputStreamReader;
            private final PipedOutputStream outputStreamWriter;
            private final InputStream errorStream;
            private final ExceptionCatchingThread thread;
            private final int id;
            private volatile boolean completed = false;

            public FakeNodeResponse(int id, java.time.Duration completetesAfter)
            {
                super(null, String.valueOf(id), NodeResponseTest.logger);
                this.id = id;
                errorStream = new ByteArrayInputStream(new byte[] {});
                outputStreamWriter = new PipedOutputStream();
                outputStreamReader = Exceptions.getUncheckedIO(() -> new PipedInputStream(outputStreamWriter));

                // Use a raw thread, not a CompletableFuture: this means we don't use any higher-level
                // thread pooling things (like CompletableFuture) that will influence the test.
                thread = new ExceptionCatchingThread(String.valueOf(id), () -> Exceptions.runUnchecked(() -> {
                    Thread.sleep(completetesAfter.toMillis());
                    outputStreamWriter.write(String.valueOf(id).getBytes(StandardCharsets.UTF_8));
                    outputStreamWriter.close();
                    completed = true;
                }));
            }

            @Override
            protected InputStream getOutputStream() throws IOException
            {
                return outputStreamReader;
            }

            @Override
            protected InputStream getErrorStream() throws IOException
            {
                return errorStream;
            }

            @Override
            public int getExitCode()
            {
                return 0;
            }

            @Override
            public boolean isCompleted()
            {
                return completed;
            }

            @Override
            protected void doKill()
            {
                thread.interrupt();
            }

            public void join()
            {
                Exceptions.runUnchecked(thread::join);
            }

            public void didNotThrow()
            {
                thread.didNotThrow();
            }
        }

        private void assertCodeCompletesWithin(java.time.Duration within, Runnable runnable)
        {
            final Instant start = Instant.now();
            runnable.run();
            assertThat(java.time.Duration.between(start, Instant.now())).isLessThan(within);
        }

        @Test
        public void many_NodeResponses_can_run_simultaneously()
        {
            nodeResponses = IntStream
                .range(0, 1024)
                .mapToObj(id -> new FakeNodeResponse(id, java.time.Duration.ofSeconds(1)))
                .collect(Collectors.toList());

            assertThat(Utils.waitForSuccess(logger, nodeResponses)).isTrue();

            nodeResponses.forEach(FakeNodeResponse::didNotThrow);
        }

        @Test
        public void waiting_for_existing_NodeResponses_does_not_block_waiting_for_others()
        {
            final int responseCount = 100;

            List<FakeNodeResponse> slowNodeResponses = IntStream
                .range(0, responseCount)
                .mapToObj(id -> new FakeNodeResponse(id, java.time.Duration.ofSeconds(3)))
                .collect(Collectors.toList());

            List<FakeNodeResponse> fastNodeResponses = IntStream
                .range(responseCount + 1, responseCount * 2)
                .mapToObj(id -> new FakeNodeResponse(id, java.time.Duration.ofSeconds(1)))
                .collect(Collectors.toList());

            nodeResponses = ImmutableList.copyOf(Iterables.concat(slowNodeResponses, fastNodeResponses));

            final ExceptionCatchingThread waitForSlowResponses = new ExceptionCatchingThread(
                "wait-slow",
                () -> Utils.waitForSuccess(logger, List.copyOf(slowNodeResponses)));

            assertCodeCompletesWithin(java.time.Duration.ofSeconds(2),
                () -> assertThat(Utils.waitForSuccess(logger, fastNodeResponses)).isTrue());

            waitForSlowResponses.didNotThrow();
            nodeResponses.forEach(FakeNodeResponse::didNotThrow);
        }

        @After
        public void cleanupNodeResponseThreads()
        {
            nodeResponses.forEach(FakeNodeResponse::kill);
            nodeResponses.forEach(FakeNodeResponse::join);
        }
    }
}
