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
package com.datastax.fallout.ops.provisioner.kubernetes;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.providers.KubeControlProvider;
import com.datastax.fallout.ops.providers.KubernetesNodeInfoProvider;
import com.datastax.fallout.runner.CheckResourcesResult;

public abstract class AbstractKubernetesProvisioner extends Provisioner
{
    private static final String KUBECONFIG_ENV_VAR = "KUBECONFIG";
    private Path kubeConfigPath;

    public NodeResponse executeInKubernetesEnv(String command)
    {
        return executeInKubernetesEnv(command, getNodeGroup().logger());
    }

    public NodeResponse executeInKubernetesEnv(String command, Logger logger)
    {
        logger.info("Executing: {}", command);
        return getCommandExecutor().executeLocally(logger, command, getKubernetesEnv());
    }

    Path getKubeConfigPath()
    {
        if (kubeConfigPath == null)
        {
            kubeConfigPath = getNodeGroup().getLocalArtifactPath().resolve("kube-config.yaml");
        }
        return kubeConfigPath;
    }

    public Map<String, String> getKubernetesEnv()
    {
        Map<String, String> kubernetesEnv = new HashMap<>();
        kubernetesEnv.put(KUBECONFIG_ENV_VAR, getKubeConfigPath().toString());
        return kubernetesEnv;
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return ImmutableSet.of(KubeControlProvider.class);
    }

    @Override
    protected CheckResourcesResult createImpl(NodeGroup nodeGroup)
    {
        if (!createKubernetesCluster(nodeGroup))
        {
            return CheckResourcesResult.FAILED;
        }
        return CheckResourcesResult.fromWasSuccessful(postCreate(nodeGroup));
    }

    protected abstract boolean createKubernetesCluster(NodeGroup nodeGroup);

    boolean postCreate(NodeGroup nodeGroup)
    {
        if (!postCreateImpl(nodeGroup))
        {
            return false;
        }

        nodeGroup.getNodes().forEach(n -> new KubeControlProvider(n, this));

        String nodeInfoCmd = "kubectl get nodes -o json";
        FullyBufferedNodeResponse nodeInfo = executeInKubernetesEnv(nodeInfoCmd).buffered();
        if (!nodeInfo.waitForSuccess())
        {
            return false;
        }

        JsonNode items = Utils.getJsonNode(nodeInfo.getStdout(), "/items");
        for (int i = 0; i < items.size(); i++)
        {
            JsonNode addresses = items.get(i).at("/status/addresses");
            String internal = null;
            String external = null;
            for (JsonNode address : addresses)
            {
                String type = address.get("type").asText();
                String val = address.get("address").asText();
                switch (type)
                {
                    case "InternalIP":
                        internal = val;
                        break;
                    case "ExternalIP":
                        external = val;
                        break;
                }
                if (external == null)
                {
                    external = internal;
                }
            }
            String nodeName = items.get(i).at("/metadata/name").asText();
            Node node = nodeGroup.getNodes().get(i);
            new KubernetesNodeInfoProvider(node, external, internal, nodeName);
        }
        return true;
    }

    protected boolean postCreateImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected boolean destroyImpl(NodeGroup nodeGroup)
    {
        nodeGroup.maybeUnregisterProviders(KubeControlProvider.class);
        return destroyKubernetesCluster(nodeGroup);
    }

    protected abstract boolean destroyKubernetesCluster(NodeGroup nodeGroup);

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        NodeGroup.State existingClusterState = checkExistingKubernetesClusterState(nodeGroup);
        if (existingClusterState == NodeGroup.State.FAILED || existingClusterState == NodeGroup.State.DESTROYED)
        {
            return existingClusterState;
        }
        if (postCreate(nodeGroup))
        {
            return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        }
        return NodeGroup.State.FAILED;
    }

    protected abstract NodeGroup.State checkExistingKubernetesClusterState(NodeGroup nodeGroup);

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected boolean stopImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected NodeResponse executeImpl(Node node, String command)
    {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> get(Node node, String remotePath, Path localPath, boolean deepCopy)
    {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, Path localPath, String remotePath, boolean deepCopy,
        int permissions)
    {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, InputStream inputStream, String remotePath, int permissions)
    {
        return CompletableFuture.completedFuture(true);
    }

    @Override
    public String getRemoteArtifactPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        return null;
    }

    @Override
    public String getRemoteScratchPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        return null;
    }

    @Override
    public String getRemoteLibraryPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        return null;
    }
}
