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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import com.datastax.fallout.ops.Node;

/** Abstracts the ability to execute a command on the fallout server which
 *  either affects a particular Node, or logs to a specified logger. */
public interface CommandExecutor
{
    class Builder
    {
        private final CommandExecutor commandExecutor;
        private Logger logger;
        private Node owner;
        private String command;
        private Map<String, String> environment = new HashMap<>();
        private Optional<Path> workingDirectory = Optional.empty();

        private Builder(CommandExecutor commandExecutor, Node owner, Logger logger, String command)
        {
            this.owner = owner;
            this.logger = logger;
            this.command = command;
            this.commandExecutor = commandExecutor;
        }

        public Builder environment(String envVar, String value)
        {
            environment.put(envVar, value);
            return this;
        }

        public Builder environment(Map<String, String> environment)
        {
            this.environment.putAll(environment);
            return this;
        }

        public Builder workingDirectory(Path workingDirectory)
        {
            this.workingDirectory = Optional.of(workingDirectory);
            return this;
        }

        public NodeResponse execute()
        {
            return owner != null ?
                commandExecutor.executeLocally(owner, command, environment, workingDirectory) :
                commandExecutor.executeLocally(logger, command, environment, workingDirectory);
        }
    }

    default Builder local(Node owner, String command)
    {
        return new Builder(this, owner, null, command);
    }

    default Builder local(Logger logger, String command)
    {
        return new Builder(this, null, logger, command);
    }

    NodeResponse executeLocally(Node owner, String command, Map<String, String> environment,
        Optional<Path> workingDirectory);

    NodeResponse executeLocally(Logger logger, String command, Map<String, String> environment,
        Optional<Path> workingDirectory);
}
