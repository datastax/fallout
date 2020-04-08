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
package com.datastax.fallout.harness;

import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.NodeCommandExecutor;
import com.datastax.fallout.ops.commands.RejectableCommandExecutor;
import com.datastax.fallout.ops.commands.RejectableNodeCommandExecutor;
import com.datastax.fallout.service.core.TestRun;

/**
 * Interface for reading whether a {@link TestRun} {@link #hasBeenAborted()}.
 *
 * <p> This is used by:
 *
 * <ul>
 * <li> {@link ActiveTestRun},
 *
 * <li> {@link AbortableModule}, which wraps a {@link Module}, to
 * prevent {@link Module}s from running if a test has been aborted
 *
 * <li> {@link RejectableNodeCommandExecutor}, which wraps a {@link NodeCommandExecutor}
 * (implemented by {@link Provisioner}), to kill active commands if a testrun {@link
 * #canBeInterruptedWithAbort}, and prevent further commands from executing during that time.
 *
 * <li> {@link RejectableCommandExecutor}, which does for {@link CommandExecutor} (local command
 * execution) what {@link RejectableNodeCommandExecutor} does for {@link NodeCommandExecutor}
 *
 * <li> {@link NodeGroup#transitionState} prevents upwards transitions when a testrun {@link #hasBeenAborted}.
 * </ul>
 */

public interface TestRunAbortedStatus
{
    boolean hasBeenAborted();

    /**
     * Returns true, when we are in a testrun state that can be shut down completely. Returns false when we are tearing
     * down a state that still needs to execute (i.e. gather artifacts, destroy clusters)
     */
    boolean canBeInterruptedWithAbort();

    default boolean currentOperationShouldBeAborted()
    {
        return hasBeenAborted() && canBeInterruptedWithAbort();
    }

    /** Add a callback that must be run as soon as the test run is told to abort i.e.
     *  not at the end of the test, but at the point of abort being signalled. */
    void addAbortListener(Runnable onAbort);
}
