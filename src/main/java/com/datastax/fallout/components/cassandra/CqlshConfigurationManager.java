/*
 * Copyright 2022 DataStax, Inc.
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

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import com.google.auto.service.AutoService;

import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.utils.ScriptUtils;

@AutoService(ConfigurationManager.class)
public class CqlshConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.cqlsh.";
    private static final String name = "cqlsh";
    private static final String description = "Configure cqlsh on client nodes";

    static private PropertySpec<String> versionSpec = PropertySpecBuilder.createStr(prefix)
        .name("version")
        .description("The version of Cqlsh to install")
        .options("cqlsh-6.8", "cqlsh-6.7", "cqlsh-6.0", "cqlsh-5.1", "cqlsh-astra")
        .defaultOf("cqlsh-astra")
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("workloads/nosqlbench/nosqlbench-example.yaml");
    }

    static public String buildCqlshUrl(String version)
    {
        return String.format("https://downloads.datastax.com/enterprise/%s.tar.gz", version);
    }

    @Override
    public void doSummarizeInfo(InfoConsumer infoConsumer)
    {
        infoConsumer.accept("version", versionSpec.value(getNodeGroup()));
    }

    @Override
    protected boolean configureImpl(NodeGroup nodeGroup)
    {
        String url = buildCqlshUrl(versionSpec.value(nodeGroup));
        return nodeGroup.waitForNodeSpecificSuccess(node -> {
            return ScriptUtils.script(
                String.format("wget %s -P %s", url, node.getRemoteScratchPath()),
                String.format("mkdir %s/cqlsh", node.getRemoteLibraryPath()),
                String.format("tar -zxvf %s/%s.tar.gz -C %s/cqlsh",
                    node.getRemoteScratchPath(), versionSpec.value(nodeGroup), node.getRemoteLibraryPath())
            );
        });
    }

    @Override
    protected boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.removeNodeSpecificLibraryDirectories("cqlsh");
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        if (nodeGroup.waitForAllNodes(
            node -> node.existsFile(Paths.get(node.getRemoteLibraryPath(), "cqlsh").toString()),
            "checking if cqlsh is installed"))
        {
            return NodeGroup.State.STARTED_SERVICES_RUNNING;
        }

        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(versionSpec);
    }
}
