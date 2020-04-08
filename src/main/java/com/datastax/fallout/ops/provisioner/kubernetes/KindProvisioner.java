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

import java.nio.file.Path;
import java.util.List;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.providers.FileProvider;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.util.YamlUtils;

@AutoService(Provisioner.class)
public class KindProvisioner extends AbstractKubernetesProvisioner
{
    private final static String KIND_VERSION = "0.7";
    private final static String prefix = "fallout.provisioner.kubernetes.kind.";

    private final static PropertySpec<String> kindConfigSpec =
        PropertySpecBuilder.createStr(prefix, FileProvider::validateIsManagedFile)
            .name("kind.config")
            .description("Kind config file to create the cluster with.")
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
        return "Provisioner for local Kubernetes In Docker. Expects KIND v" + KIND_VERSION + " is already installed.";
    }

    @Override
    public boolean disabledWhenShared()
    {
        return true;
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .addAll(super.getPropertySpecs())
            .add(kindConfigSpec)
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        FullyBufferedNodeResponse getKindVersion = getNodeGroup().getProvisioner().getCommandExecutor()
            .executeLocally(getNodeGroup().logger(), "kind --version").buffered();
        if (!getKindVersion.waitForSuccess())
        {
            throw new PropertySpec.ValidationException("Could not look up the version of KIND. Is it installed?");
        }
        if (!getKindVersion.getStderr().contains(KIND_VERSION))
        {
            throw new PropertySpec.ValidationException(String.format(
                "Incorrect version of KIND installed. Fallout requires v%s, but found %s",
                KIND_VERSION, getKindVersion.getStderr()));
        }
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
            Path submittedKindConfig =
                nodeGroup.findFirstRequiredProvider(FileProvider.LocalFileProvider.class).getFullPath(fileMarker);

            int nodesInConfig =
                ((List<String>) YamlUtils.loadYaml(Utils.readStringFromFile(submittedKindConfig.toFile())).get("nodes"))
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
        Utils.writeStringToFile(getKubeConfigPath().toFile(), getKubeConfig.getStdout());
        return true;
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
