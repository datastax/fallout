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
