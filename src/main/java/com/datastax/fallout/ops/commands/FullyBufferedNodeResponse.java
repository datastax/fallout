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

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Helper class for instances that want
 * simple access to fully buffered stdin/stderr
 */
public class FullyBufferedNodeResponse extends NodeResponse
{
    final NodeResponse delegate;
    final List<String> stdoutLines;
    final List<String> stdErrLines;

    String stdout = null;
    String stderr = null;

    FullyBufferedNodeResponse(NodeResponse delegate)
    {
        super(delegate.getOwner(), delegate.getCommand(), delegate.logger);
        this.delegate = delegate;
        this.stdoutLines = new LinkedList<>();
        this.stdErrLines = new LinkedList<>();
    }

    public String getStdout()
    {
        if (!isCompleted())
            throw new IllegalStateException("Only call after process is complete");

        if (stdout == null)
        {
            stdout = String.join("\n", stdoutLines);
            stdoutLines.clear();
        }
        return stdout;
    }

    public String getStderr()
    {
        if (!isCompleted())
            throw new IllegalStateException("Only call after process is complete");

        if (stderr == null)
        {
            stderr = String.join("\n", stdErrLines);
            stdErrLines.clear();
        }
        return stderr;
    }

    public <T> CompletableFuture<T> processStdoutAsync(Function<String, T> processor)
    {
        return getStdoutAsync().thenApplyAsync(processor);
    }

    public CompletableFuture<String> getStdoutAsync()
    {
        return doWait().forProcessEndAsync().thenApplyAsync(ignored -> getStdout());
    }

    @Override
    public FullyBufferedNodeResponse buffered()
    {
        return this;
    }

    @Override
    protected WaitOptions createWaitOptions()
    {
        WaitOptions res = super.createWaitOptions();
        res.stdoutConsumer = outLine -> stdoutLines.add(outLine);
        res.stderrConsumer = errLine -> stdErrLines.add(errLine);
        return res;
    }

    @Override
    protected InputStream getOutputStream() throws IOException
    {
        return delegate.getOutputStream();
    }

    @Override
    protected InputStream getErrorStream() throws IOException
    {
        return delegate.getErrorStream();
    }

    @Override
    public int getExitCode()
    {
        return delegate.getExitCode();
    }

    @Override
    public boolean isCompleted()
    {
        return delegate.isCompleted();
    }

    @Override
    public void doKill()
    {
        delegate.kill();
    }

    @Override
    public String maybeAppendCommandOutputToLogMessage(String logMsg)
    {
        String res = logMsg;
        if (!getStdout().isEmpty())
        {
            res += "\nSTDOUT:\n" + getStdout();
        }
        if (!getStderr().isEmpty())
        {
            res += "\nSTDERR:\n" + getStderr();
        }
        return res;
    }
}
