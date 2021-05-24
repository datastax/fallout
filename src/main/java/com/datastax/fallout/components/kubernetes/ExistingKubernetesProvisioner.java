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
package com.datastax.fallout.components.kubernetes;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.runner.CheckResourcesResult;

@AutoService(Provisioner.class)
public class ExistingKubernetesProvisioner extends AbstractKubernetesProvisioner
{
    private static final String prefix = "fallout.provisioner.kubernetes.existing.";

    private static final PropertySpec<FileProvider.LocalManagedFileRef> kubeConfigSpec =
        PropertySpecBuilder.createLocalManagedFileRef(prefix)
            .name("kubeconfig")
            .description(
                "kubeconfig for connecting to the existing kubernetes cluster. Can be omitted if Fallout is running in the target cluster.")
            .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "existing_kubernetes";
    }

    @Override
    public String description()
    {
        return "Either uses a kubeconfig to establish connection to an existing kubernetes cluster or, if running within the target cluster, attempts to connect to the api-server directly. If the number of nodes in the kubernetes cluster is unknown, set the node.count to 1.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("kubernetes/existing-cluster.yaml");
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(kubeConfigSpec);
    }

    @Override
    Optional<Path> getOptionalKubeConfigPath()
    {
        return kubeConfigSpec.optionalValue(getNodeGroup()).map(kc -> kc.fullPath(getNodeGroup()));
    }

    @Override
    protected NodeGroup.State checkExistingKubernetesClusterState(NodeGroup nodeGroup)
    {
        // Check if we can execute an arbitrary kubectl command using the supplied kube config.
        return executeInKubernetesEnv("kubectl get pods").waitForSuccess() ?
            NodeGroup.State.STARTED_SERVICES_UNCONFIGURED : NodeGroup.State.FAILED;
    }

    /**
     * Allow for unknown nodegroup size by creating KubernetesNodeInfoProvider for only the nodes in the node group
     * whose ordinal have a corresponding node info in the cluster nodes list. See FAL-1623.
     */
    @Override
    protected int getEffectiveNodeCount(NodeGroup nodeGroup, JsonNode nodesList)
    {
        int declared = nodeGroup.getNodes().size();
        int discovered = nodesList.size();
        if (declared == discovered)
        {
            return discovered;
        }

        int effective = Math.min(declared, discovered);
        nodeGroup.logger().info(
            "Detected mismatch between the number of nodes declared ({}) and the number of nodes discovered ({}). Setting effective node count to: {}",
            declared, discovered, effective);
        if (declared != 1)
        {
            nodeGroup.logger().warn(
                "Tests against existing kubernetes cluster of an unknown number of nodes should set the node.count to 1. See FAL-1623.");
        }
        return effective;
    }

    @Override
    protected boolean createKubernetesCluster(NodeGroup nodeGroup)
    {
        return true; // no-op
    }

    @Override
    protected boolean destroyKubernetesCluster(NodeGroup nodeGroup)
    {
        return true; // no-op
    }

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.AVAILABLE;
    }
}
