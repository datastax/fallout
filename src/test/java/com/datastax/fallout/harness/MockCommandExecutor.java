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
package com.datastax.fallout.harness;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Exceptions;

/** Implementation of CommandExecutor that allows mocking the exit codes and output
 *  of commands using a list of commands created with {@link #local} */
public class MockCommandExecutor implements CommandExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(MockCommandExecutor.class);

    public static class MockCommandResponse
    {
        private final Predicate<String> matches;
        private int exitCode = 0;
        private String stderrContent = "";
        private Consumer<PrintWriter> stderrOutputStreamWriter = writer -> writer.print(stderrContent);
        private Optional<Duration> duration = Optional.empty();

        private MockCommandResponse(String regex)
        {
            this.matches = Pattern.compile(regex).asPredicate();
        }

        public MockCommandResponse exitsWith(int exitCode)
        {
            this.exitCode = exitCode;
            return this;
        }

        public MockCommandResponse outputsOnStderr(String output)
        {
            this.stderrContent = output;
            return this;
        }

        public MockCommandResponse exitsAfter(Duration duration)
        {
            this.duration = Optional.of(duration);
            return this;
        }

        /** stderrOutputStreamWriter will be called on a separate thread */
        public MockCommandResponse outputsOnStderr(Consumer<PrintWriter> stderrOutputStreamWriter)
        {
            this.stderrOutputStreamWriter = stderrOutputStreamWriter;
            return this;
        }

        public NodeResponse execute()
        {
            return new MockNodeResponse(this, "mock", logger);
        }

        private CompletableFuture<Integer> createExitCodeFuture()
        {
            var durationFuture = duration.map(duration_ -> CompletableFuture.supplyAsync(() -> exitCode,
                CompletableFuture.delayedExecutor(duration_.toNanos(), TimeUnit.NANOSECONDS)));

            return durationFuture.orElse(CompletableFuture.completedFuture(exitCode));
        }
    }

    /** Mock any command that matches regex */
    public static MockCommandResponse command(String regex)
    {
        return new MockCommandResponse(regex);
    }

    /** Create a command that matches everything; this can also be used in
     *  a standalone mode with {@link MockCommandResponse#execute()} */
    public static MockCommandResponse command()
    {
        return command("");
    }

    private final MockCommandResponse defaultCommandResponse;
    private final List<MockCommandResponse> commandResponses;

    /** Create a MockCommandExecutor that will respond to commands according
     *  to the commandResponses created with {@link #local}.  Commands not matching any of
     *  commandResponses will have an exit code of defaultExitCode. */
    public MockCommandExecutor(int defaultExitCode, MockCommandResponse... commandResponses)
    {
        this.defaultCommandResponse = command().exitsWith(defaultExitCode);
        this.commandResponses = List.of(commandResponses);
    }

    /** Equivalent to calling MockCommandExecutor(0, commandResponses) */
    public MockCommandExecutor(MockCommandResponse... commandResponses)
    {
        this(0, commandResponses);
    }

    @Override
    public NodeResponse executeLocally(Node owner, String command, Map<String, String> environment,
        Optional<Path> workingDirectory)
    {
        return new MockNodeResponse(executeCommand(command), command, owner.logger());
    }

    @Override
    public NodeResponse executeLocally(Logger logger, String command, Map<String, String> environment,
        Optional<Path> workingDirectory)
    {
        return new MockNodeResponse(executeCommand(command), command, logger);
    }

    private MockCommandResponse executeCommand(String command)
    {
        return commandResponses.stream()
            .filter(commandResponse -> commandResponse.matches.test(command))
            .findFirst()
            .orElse(defaultCommandResponse);
    }

    private static class MockNodeResponse extends NodeResponse
    {
        private final long startTime = System.nanoTime();
        private final CompletableFuture<Integer> exitCodeFuture;
        private final PipedInputStream stderrInputStream;

        private MockNodeResponse(MockCommandResponse mockCommandResponse, String command, Logger logger)
        {
            super(null, command, logger);

            // Connect both ends of the pipe before we start the thread that will write to the pipe, and just as
            // importantly, before we return from this constructor and anything tries to read from stderrInputStream.
            stderrInputStream = new PipedInputStream();
            var stderrOutputWriter = new PrintWriter(new OutputStreamWriter(
                Exceptions.getUncheckedIO(() -> new PipedOutputStream(stderrInputStream)),
                StandardCharsets.UTF_8));

            var stderrFuture = CompletableFuture.runAsync(() -> {
                try (var writer = stderrOutputWriter)
                {
                    mockCommandResponse.stderrOutputStreamWriter.accept(writer);
                }
            });

            exitCodeFuture = mockCommandResponse.createExitCodeFuture()
                .thenComposeAsync(exitCode -> stderrFuture
                    .thenApplyAsync(v -> exitCode));
        }

        @Override
        protected InputStream getOutputStream()
        {
            return new ByteArrayInputStream(new byte[] {});
        }

        @Override
        protected InputStream getErrorStream()
        {
            return stderrInputStream;
        }

        @Override
        public int getExitCode()
        {
            return exitCodeFuture.join();
        }

        @Override
        public boolean isCompleted()
        {
            return exitCodeFuture.isDone();
        }

        @Override
        public void doKill()
        {
            exitCodeFuture.complete(-1);
        }
    }
}
