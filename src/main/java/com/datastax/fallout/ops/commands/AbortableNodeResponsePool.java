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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.datastax.fallout.harness.TestRunAbortedStatus;
import com.datastax.fallout.ops.Node;

/** Adds {@link NodeResponse}s to a pool and provides a way to {@link NodeResponse#kill} all of them. */
class AbortableNodeResponsePool
{
    private final TestRunAbortedStatus testRunAbortedStatus;
    private final Set<NodeResponse> pool = new HashSet<>();

    AbortableNodeResponsePool(TestRunAbortedStatus testRunAbortedStatus)
    {
        this.testRunAbortedStatus = testRunAbortedStatus;
        testRunAbortedStatus.addAbortListener(this::killActiveCommands);
    }

    private boolean commandsShouldBeRejected()
    {
        return testRunAbortedStatus.currentOperationShouldBeAborted();
    }

    synchronized private NodeResponse addActiveCommand(NodeResponse nodeResponse)
    {
        pool.add(nodeResponse);
        nodeResponse.addCompletionListener(nodeResponse_ -> {
            synchronized (this)
            {
                pool.remove(nodeResponse_);
            }
        });
        return nodeResponse;
    }

    public NodeResponse addOrRejectCommand(Node node, String command, Supplier<NodeResponse> executor)
    {
        if (commandsShouldBeRejected())
        {
            return new NodeResponse.ErrorResponse(node, "Test aborted; not running command: " + command);
        }
        return addActiveCommand(executor.get());
    }

    public NodeResponse addOrRejectCommand(Logger logger, String command, Supplier<NodeResponse> executor)
    {
        if (commandsShouldBeRejected())
        {
            return new NodeResponse.ErrorResponse(logger, "Test aborted; not running command: " + command);
        }
        return addActiveCommand(executor.get());
    }

    synchronized private void killActiveCommands()
    {
        if (testRunAbortedStatus.canBeInterruptedWithAbort())
        {
            pool.forEach(NodeResponse::kill);
        }
    }
}
