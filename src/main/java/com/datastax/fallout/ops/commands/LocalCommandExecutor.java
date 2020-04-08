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
package com.datastax.fallout.ops.commands;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Utils;

public class LocalCommandExecutor implements CommandExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(LocalCommandExecutor.class);

    @Override
    public NodeResponse executeLocally(Node owner, String command, Map<String, String> environment)
    {
        Process proc = executeShell(command, environment);
        return new LocalNodeResponse(owner, command, proc);
    }

    @Override
    public NodeResponse executeLocally(Logger logger, String command, Map<String, String> environment)
    {
        Process proc = executeShell(command, environment);
        return new LocalNodeResponse(logger, command, proc);
    }

    private Process executeShell(String command, Map<String, String> environment)
    {
        logger.info("Executing locally: " + command);
        ProcessBuilder pb = new ProcessBuilder(Utils.wrapCommandWithBash(command, false));
        pb.environment().putAll(environment);
        try
        {
            return pb.start();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static class LocalNodeResponse extends NodeResponse
    {
        private final Process p;
        private final InputStream inputStream;
        private final InputStream errorStream;

        private LocalNodeResponse(Logger logger, String command, Process p)
        {
            super(null, command, logger);

            this.p = p;
            this.inputStream = new BufferedInputStream(p.getInputStream());
            this.errorStream = new BufferedInputStream(p.getErrorStream());
        }

        private LocalNodeResponse(Node owner, String command, Process p)
        {
            super(owner, command);

            this.p = p;
            this.inputStream = new BufferedInputStream(p.getInputStream());
            this.errorStream = new BufferedInputStream(p.getErrorStream());
        }

        @Override
        public InputStream getOutputStream() throws IOException
        {
            return inputStream;
        }

        @Override
        public InputStream getErrorStream() throws IOException
        {
            return errorStream;
        }

        @Override
        public int getExitCode()
        {
            return p.exitValue();
        }

        @Override
        public boolean isCompleted()
        {
            return !p.isAlive();
        }

        @Override
        public void doKill()
        {
            Stream.concat(p.descendants(), Stream.of(p.toHandle())).forEach(processHandle -> {
                logger.debug("Killing {} {}", processHandle.pid(), processHandle.info().command());
                processHandle.destroyForcibly();
            });
        }
    }
}
