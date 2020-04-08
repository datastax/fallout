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
package com.datastax.fallout.harness.modules.nosqlbench;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.providers.KubeControlProvider;

public class NoSqlBenchProvider extends Provider
{
    private final Optional<String> namespace;
    private final Path podArtifactsDir;
    private final String podLabel;

    public NoSqlBenchProvider(Node node, Optional<String> namespace, Path podArtifactsDir, String podLabel)
    {
        super(node);
        this.namespace = namespace;
        this.podArtifactsDir = podArtifactsDir;
        this.podLabel = podLabel;
    }

    @Override
    public String name()
    {
        return "nosqlbench";
    }

    public Optional<String> getNamespace()
    {
        return namespace;
    }

    public Path getPodArtifactsDir()
    {
        return podArtifactsDir;
    }

    NodeResponse nosqlbench(String moduleName, List<String> args)
    {
        String nosqlBenchArgs = String.join(" ", args);
        Path log = podArtifactsDir.resolve(String.format("%s.log", moduleName));
        return node.getProvider(KubeControlProvider.class).inNamespace(namespace, namespacedKubeCtl -> {
            String podName = namespacedKubeCtl.findPodNames(podLabel, true).get(0);
            return namespacedKubeCtl.execute(String.format(
                "exec --container=nosqlbench %s -- ash -c 'java -jar /target/nb.jar %s 2>&1 | tee %s'",
                podName, nosqlBenchArgs, log));
        });
    }
}
