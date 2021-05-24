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
package com.datastax.fallout.components.common.provider;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;

public class LocalShellProvider extends ShellProvider
{
    private final CommandExecutor commandExecutor;

    public LocalShellProvider(Node node, CommandExecutor commandExecutor)
    {
        super(node);
        this.commandExecutor = commandExecutor;
    }

    @Override
    public String name()
    {
        return "local_bash_provider";
    }

    @Override
    public NodeResponse execute(String command)
    {
        return commandExecutor.local(node(), command).execute();
    }
}
