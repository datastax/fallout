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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.ops.ClusterNames;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.YamlUtils;

@AutoService(Provisioner.class)
public class KindProvisioner extends AbstractKubernetesProvisioner
{
    private final static String prefix = "fallout.provisioner.kubernetes.kind.";

    private final static PropertySpec<FileProvider.LocalManagedFileRef> kindConfigSpec =
        PropertySpecBuilder.createLocalManagedFileRef(prefix)
            .name("kind.config")
            .description("Kind config file to create the cluster with.")
            .build();

    private final static PropertySpec<List<String>> kindLoadSpec =
        PropertySpecBuilder.createStrList(prefix)
            .name("image.load")
            .description("The list of docker images to pull then load into this kind cluster")
            .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "kind";
    }

    @Override
    public String description()
    {
        return "Provisioner for local Kubernetes In Docker.";
    }

    @Override
    public boolean disabledWhenShared()
    {
        return true;
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .addAll(super.getPropertySpecs())
            .add(kindConfigSpec)
            .add(kindLoadSpec)
            .build();
    }

    @Override
    public String generateClusterName(NodeGroup nodeGroup, Optional<User> user, TestRunIdentifier testRunIdentifier)
    {
        return ClusterNames.generateGCEClusterName(nodeGroup, testRunIdentifier);
    }

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.AVAILABLE;
    }

    @Override
    protected boolean createKubernetesCluster(NodeGroup nodeGroup)
    {
        String kindConfigArg = kindConfigSpec.optionalValue(nodeGroup).map(fileMarker -> {
            Path submittedKindConfig = fileMarker.fullPath(nodeGroup);
            int nodesInConfig =
                ((List<String>) YamlUtils.loadYaml(FileUtils.readString(submittedKindConfig)).get("nodes"))
                    .size();
            if (nodesInConfig != nodeGroup.getNodes().size())
            {
                throw new RuntimeException(
                    String.format("Submitted kind config defined %d nodes, but the node.count was %s", nodesInConfig,
                        nodeGroup.getNodes().size()));
            }

            return String.format(" --config %s", submittedKindConfig);
        })
            .orElse("");

        return executeInKubernetesEnv(
            String.format("kind create cluster --name %s %s", clusterName(nodeGroup), kindConfigArg))
                .waitForSuccess();
    }

    @Override
    protected boolean postCreateImpl(NodeGroup nodeGroup)
    {
        FullyBufferedNodeResponse getKubeConfig = executeInKubernetesEnv(
            String.format("kind get kubeconfig --name='%s'", clusterName(nodeGroup))).buffered();
        if (!getKubeConfig.waitForSuccess())
        {
            return false;
        }
        FileUtils.writeString(getKubeConfigPath(), getKubeConfig.getStdout());
        return kindLoadSpec.optionalValue(nodeGroup)
            .map(images -> images.stream().allMatch(image -> executeInKubernetesEnv(
                String.format("kind load docker-image %s --name %s", image, clusterName(nodeGroup))).waitForSuccess()))
            .orElse(true);
    }

    @Override
    protected boolean destroyKubernetesCluster(NodeGroup nodeGroup)
    {
        return executeInKubernetesEnv(String.format("kind delete cluster --name %s", clusterName(nodeGroup)))
            .waitForSuccess();
    }

    @Override
    protected NodeGroup.State checkExistingKubernetesClusterState(NodeGroup nodeGroup)
    {
        FullyBufferedNodeResponse getClusters = executeInKubernetesEnv("kind get clusters").buffered();
        if (!getClusters.waitForSuccess())
        {
            return NodeGroup.State.FAILED;
        }
        if (!getClusters.getStdout().contains(clusterName(nodeGroup)))
        {
            return NodeGroup.State.DESTROYED;
        }
        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }
}
