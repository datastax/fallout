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
package com.datastax.fallout.components.nosqlbench;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import com.datastax.fallout.components.kubernetes.KubeControlProvider;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;

public class NoSqlBenchPodProvider extends NoSqlBenchProvider
{
    private final Optional<String> namespace;
    private final Path podArtifactsDir;
    private final String podLabel;

    public NoSqlBenchPodProvider(Node node, Optional<String> namespace, Path podArtifactsDir, String podLabel)
    {
        super(node);
        this.namespace = namespace;
        this.podArtifactsDir = podArtifactsDir;
        this.podLabel = podLabel;
    }

    @Override
    public String name()
    {
        return "nosqlbench_pod";
    }

    public Optional<String> getNamespace()
    {
        return namespace;
    }

    @Override
    protected Path getBaseArtifactDir()
    {
        return podArtifactsDir;
    }

    @Override
    public NodeResponse nosqlbench(String moduleName, String prepareScript, List<String> args,
        Duration histogramFrequency)
    {
        String nosqlbenchArgs = buildNosqlbenchArgs(moduleName, args, histogramFrequency);
        return node().getProvider(KubeControlProvider.class).inNamespace(namespace, kubeCtl -> {
            String podName = kubeCtl.findPodNames(podLabel, true).get(0);
            return kubeCtl.execute(String
                .format("exec --container=nosqlbench %s -- ash -o pipefail -c '%s java -jar nb.jar %s'", podName,
                    prepareScript, nosqlbenchArgs));
        });
    }

    // TODO: Actually implement this!
    protected String fetchVersionInfo()
    {
        return "unknown";
    }
}
