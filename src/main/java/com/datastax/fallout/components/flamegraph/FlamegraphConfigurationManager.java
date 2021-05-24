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
package com.datastax.fallout.components.flamegraph;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;

import com.google.auto.service.AutoService;

import com.datastax.fallout.components.common.provider.JavaProvider;
import com.datastax.fallout.components.common.spec.GitClone;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.utils.ScriptUtils;

/**
 * Installs java-perf-map on the nodes and creates the provider
 */
@AutoService(ConfigurationManager.class)
public class FlamegraphConfigurationManager extends ConfigurationManager
{
    @Override
    public String prefix()
    {
        return "fallout.configuration.management.flamegraph.";
    }

    @Override
    public String name()
    {
        return "flamegraph";
    }

    @Override
    public String description()
    {
        return "Sets up flamegraphing projects";
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(FlamegraphProvider.class);
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(JavaProvider.class);
    }

    @Override
    protected boolean configureImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.waitForNodeSpecificSuccess(node -> {
            String libraryDir = node.getRemoteLibraryPath();
            return ScriptUtils.script(
                "set -e",
                GitClone.statement("ssh://git@github.com/riptano/FlameGraph",
                    Paths.get(libraryDir, "FlameGraph").toString()),
                GitClone.statement("ssh://git@github.com/riptano/perf-map-agent",
                    Paths.get(libraryDir, "perf-map-agent").toString()),
                "sudo su -c 'DEBIAN_FRONTEND=noninteractive apt-get --assume-yes install cmake linux-tools-common linux-tools-generic linux-tools-`uname -r`'",
                "cmake .",
                "make",
                "echo 'export FLAMEGRAPH_DIR=" + libraryDir + "/FlameGraph' >> " + libraryDir + "/flamegraph.env",
                "echo 'export PERF_MAP_OPTIONS=unfoldall,cleanclass,annotate_java_frames' >> " + libraryDir +
                    "/flamegraph.env",
                "echo 'export PATH=\"$PATH:" + libraryDir + "/perf-map-agent/bin:$JAVA_HOME/bin\"' >> " + libraryDir +
                    "/flamegraph.env");
        });
    }

    @Override
    public boolean registerProviders(Node node)
    {
        new FlamegraphProvider(node, node.getRemoteLibraryPath() + "/flamegraph.env",
            node.getRemoteScratchPath() + "/flamegraphs");
        return true;
    }

    @Override
    public boolean unregisterProviders(Node node)
    {
        node.maybeUnregister(FlamegraphProvider.class);
        return true;
    }

    @Override
    protected boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.removeNodeSpecificLibraryDirectories("FlameGraph", "perf-map-agent");
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        boolean flamegraphEnvExists = nodeGroup.waitForNodeSpecificSuccess(
            node -> String.format("ls %s | grep 'flamegraph.env'", node.getRemoteLibraryPath()),
            wo -> {
                wo.noOutputTimeout = Optional.empty();
                wo.nonZeroIsNoError();
            });

        if (flamegraphEnvExists)
        {
            return NodeGroup.State.STARTED_SERVICES_RUNNING;
        }
        else
        {
            return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        }
    }

    @Override
    protected boolean prepareArtifactsImpl(Node node)
    {
        return node.getProvider(FlamegraphProvider.class).postProcessData();
    }

    @Override
    protected boolean collectArtifactsImpl(Node node)
    {
        return node.waitForSuccess(String.format("cp -rv %s/flamegraphs %s/flamegraphs",
            node.getRemoteScratchPath(), node.getRemoteArtifactPath()));
    }
}
