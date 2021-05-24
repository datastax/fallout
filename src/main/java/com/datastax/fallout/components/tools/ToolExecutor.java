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
package com.datastax.fallout.components.tools;

import java.nio.file.Path;
import java.util.Optional;

import org.slf4j.Logger;

import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;

public class ToolExecutor
{
    private final CommandExecutor commandExecutor;
    private final Path toolsDir;

    public ToolExecutor(CommandExecutor commandExecutor, Path toolsDir)
    {
        this.commandExecutor = commandExecutor;
        this.toolsDir = toolsDir;
    }

    /** Because we set the working directory, we need to make sure we're using an absolute path */
    private Path getAbsoluteToolPath(String category, String name)
    {
        return toolsDir.resolve(category).resolve("bin").resolve(name).toAbsolutePath();
    }

    NodeResponse executeTool(Logger logger, String category, Path workingDirectory, String name, Optional<String> args)
    {
        return commandExecutor
            .local(logger, String.format("%s%s",
                getAbsoluteToolPath(category, name),
                args.map(args_ -> " " + args_).orElse("")))
            .workingDirectory(workingDirectory)
            .execute();
    }
}
