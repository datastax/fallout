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

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import com.datastax.fallout.harness.TestRunAbortedStatus;
import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.ops.Node;

/** Wraps a {@link CommandExecutor} and gates calls to it based on a supplied {@link TestRunStatus} */
public class RejectableCommandExecutor implements CommandExecutor
{
    private final CommandExecutor delegate;
    private final AbortableNodeResponsePool pool;

    public RejectableCommandExecutor(TestRunAbortedStatus testRunAbortedStatus,
        CommandExecutor delegate)
    {
        pool = new AbortableNodeResponsePool(testRunAbortedStatus);
        this.delegate = delegate;
    }

    public NodeResponse executeLocally(Node owner, String command, Map<String, String> environment,
        Optional<Path> workingDirectory)
    {
        return pool.addOrRejectCommand(owner, command,
            () -> delegate.executeLocally(owner, command, environment, workingDirectory));
    }

    public NodeResponse executeLocally(Logger logger, String command, Map<String, String> environment,
        Optional<Path> workingDirectory)
    {
        return pool.addOrRejectCommand(logger, command,
            () -> delegate.executeLocally(logger, command, environment, workingDirectory));
    }
}
