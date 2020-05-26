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
package com.datastax.fallout.harness.modules.chaos_mesh;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.providers.KubeControlProvider;

public class ChaosMeshProvider extends Provider
{
    ChaosMeshProvider(Node node)
    {
        super(node);
    }

    @Override
    public String name()
    {
        return "chaos_mesh";
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        return Set.of(KubeControlProvider.class);
    }

    NodeResponse startExperiment(Path experiment, Optional<String> namespace)
    {
        return doInNamespace(namespace, namespacedKubeCtl -> namespacedKubeCtl.applyManifest(experiment));
    }

    NodeResponse stopExperiment(Path experiment, Optional<String> namespace)
    {
        return doInNamespace(namespace, namespacedKubeCtl -> namespacedKubeCtl.deleteResource(experiment));
    }

    private NodeResponse doInNamespace(Optional<String> namespace,
        Function<KubeControlProvider.NamespacedKubeCtl, NodeResponse> action)
    {
        return node.getProvider(KubeControlProvider.class).inNamespace(namespace, action);
    }
}
