/*
 * Copyright 2024 DataStax, Inc.
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
package com.datastax.fallout.components.cassandra;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.google.auto.service.AutoService;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;

@AutoService(Provider.class)
public class RemoteCqlshProvider extends Provider
{
    private final String cqlshPath;
    private final String cqlshVersion;

    public RemoteCqlshProvider(Node node, String cqlshPath, String cqlshVersion)
    {
        super(node);
        this.cqlshPath = cqlshPath;
        this.cqlshVersion = cqlshVersion;
    }

    @Override
    public String name()
    {
        return "cqlsh";
    }

    public String getCqlshVersion()
    {
        return cqlshVersion;
    }

    public NodeResponse cqlsh(String cql)
    {
        return cqlsh(cql, null, null, Collections.emptyList(), Optional.empty());
    }

    public NodeResponse cqlsh(String cql, String username, String password, Optional<Path> outputFilePath)
    {
        return cqlsh(cql, username, password, Collections.emptyList(), outputFilePath);
    }

    public NodeResponse cqlsh(String cql, String username, String password, List<String> extraParams,
        Optional<Path> outputFilePath)
    {
        return cqlsh(cql, username, password, extraParams, false, outputFilePath);
    }

    public NodeResponse cqlsh(String cqlOrScript, String username, String password, List<String> extraParams,
        boolean runScript, Optional<Path> outputFilePath)
    {

        String fullCqlParams = "--request-timeout=60"; // default is 10s, 60s as we usually run DDL operations
        if (username != null && password != null)
        {
            fullCqlParams = fullCqlParams.concat(String.format(" -u %s -p %s", username, password));
        }
        for (String param : extraParams)
        {
            fullCqlParams = fullCqlParams.concat(String.format(" %s", param));
        }
        if (outputFilePath.isPresent())
        {
            fullCqlParams = fullCqlParams.concat(String.format(" | tee -a %s", outputFilePath.get()));
        }
        if (runScript)
            return runScript(cqlOrScript, fullCqlParams);
        else
            return runTool(cqlOrScript, fullCqlParams);
    }

    // these are basically cribbed from existing CassandraProvider
    private FullyBufferedNodeResponse runTool(String stdIn, String fullCqlParams)
    {
        String toolCmd = cqlshPath + " " + fullCqlParams;
        if (stdIn != null)
        {
            toolCmd = "echo \"" + stdIn + "\" | " + toolCmd;
        }
        return node().executeBuffered(toolCmd);
    }

    private FullyBufferedNodeResponse runScript(String scriptPath, String fullCqlParams)
    {
        String toolCmd = cqlshPath + " " + fullCqlParams;
        if (scriptPath != null)
        {
            toolCmd = "cat " + scriptPath + " | " + toolCmd;
        }
        return node().executeBuffered(toolCmd);
    }
}
