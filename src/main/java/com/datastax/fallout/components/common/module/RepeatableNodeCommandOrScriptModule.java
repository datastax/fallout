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
package com.datastax.fallout.components.common.module;

import java.util.List;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;

/**
 * Basic module to run repeatable commands or scripts.
 */
public abstract class RepeatableNodeCommandOrScriptModule<CP extends Provider> extends RepeatableNodeCommandModule<CP>
{
    private boolean executeScript = false;
    private String scriptDestination;
    private String targetScriptName = "script";

    public RepeatableNodeCommandOrScriptModule(Class<CP> commandProviderClass, String prefix)
    {
        super(commandProviderClass, prefix);
    }

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        List<String> downloadPrefixes = List.of("http://", "https://", "ftp://");
        String command = super.getCommand(properties);
        NodeGroup target = ensemble.getNodeGroupByAlias(getNodesSpec().getNodeGroupSpec().value(properties));

        if (FileProvider.isManagedFileRef(command))
        {
            executeScript = true;
            scriptDestination = target.findFirstRequiredProvider(FileProvider.RemoteFileProvider.class)
                .getFullPath(command).toString();
        }
        else if (downloadPrefixes.stream().anyMatch(command::startsWith))
        {
            executeScript = true;
            NodeGroup nodeGroup = ensemble.getServerGroup(super.getNodesSpec().getNodeGroupSpec(), properties);
            scriptDestination =
                nodeGroup.findFirstNodeWithRequiredProvider(getCommandProviderClass()).getRemoteScratchPath() +
                    "/" +
                    targetScriptName;
            try
            {
                boolean downloadSuccess = nodeGroup.download(command, scriptDestination);
                if (downloadSuccess)
                {
                    logger().info("Successfully downloaded script file from {} to {}", command, scriptDestination);
                }
                else
                {
                    throw new IllegalStateException("An error occurred while downloading the script file");
                }
            }
            catch (Exception e)
            {
                throw new IllegalStateException("An error occurred while downloading the script file");
            }
        }
        else if (target.waitForAllNodes(n -> n.existsFile(command), "checking for existing script"))
        {
            executeScript = true;
            scriptDestination = command;
        }
        // need to initialize other private variables in parent class
        super.setup(ensemble, properties);
    }

    public boolean executeScript()
    {
        return executeScript;
    }

    public String getScriptDestination()
    {
        return scriptDestination;
    }
}
