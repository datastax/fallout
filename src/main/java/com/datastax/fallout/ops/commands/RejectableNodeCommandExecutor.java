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

import com.datastax.fallout.harness.TestRunAbortedStatus;
import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.ops.Node;

/** Wraps a {@link CommandExecutor} and gates calls to it based on a supplied {@link TestRunStatus} */
public class RejectableNodeCommandExecutor implements NodeCommandExecutor
{
    private final NodeCommandExecutor delegate;
    private final AbortableNodeResponsePool pool;

    public RejectableNodeCommandExecutor(TestRunAbortedStatus testRunAbortedStatus,
        NodeCommandExecutor delegate)
    {
        pool = new AbortableNodeResponsePool(testRunAbortedStatus);
        this.delegate = delegate;
    }

    @Override
    public NodeResponse execute(Node node, String command)
    {
        return pool.addOrRejectCommand(node, command, () -> delegate.execute(node, command));
    }
}
