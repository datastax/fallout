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

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;

import com.datastax.fallout.ops.Node;

/** Abstracts the ability to execute a command on the fallout server which
 *  either affects a particular Node, or logs to a specified logger. */
public interface CommandExecutor
{
    default NodeResponse executeLocally(Node owner, String command)
    {
        return executeLocally(owner, command, Collections.emptyMap());
    }

    default NodeResponse executeLocally(Logger logger, String command)
    {
        return executeLocally(logger, command, Collections.emptyMap());
    }

    NodeResponse executeLocally(Node owner, String command, Map<String, String> environment);

    NodeResponse executeLocally(Logger logger, String command, Map<String, String> environment);
}
