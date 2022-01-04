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
package com.datastax.fallout.components.common.provisioner;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.common.provider.SshProvider;
import com.datastax.fallout.ops.FalloutPropertySpecs;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.runner.CheckResourcesResult;

/**
 * A provisioner that only points to a single host over ssh
 */
@AutoService(Provisioner.class)
public class SshOnlyProvisioner extends AbstractSshProvisioner implements Provisioner.SingleNodeProvisioner
{
    static final String prefix = "fallout.provisioner.sshonly.";

    static final PropertySpec<Integer> sshPortSpec = PropertySpecBuilder.createInt(prefix)
        .name("port")
        .description("Port for ssh connection")
        .defaultOf(22)
        .build();

    static final PropertySpec<String> sshHostSpec = PropertySpecBuilder.createStr(prefix)
        .name("host")
        .description("Host for ssh connection")
        .required()
        .build();

    static final PropertySpec<String> userPasswordSpec = PropertySpecBuilder.createStr(prefix)
        .name("user.password")
        .description("Password for ssh connection")
        .build();

    static final PropertySpec<String> userPropertySpec = PropertySpecBuilder.createStr(prefix)
        .name("user.name")
        .description("User name")
        .alias(FalloutPropertySpecs.userPropertySpec.name())
        .required()
        .build();

    static final PropertySpec<String> artifactSpec = PropertySpecBuilder.createStr(prefix)
        .name("artifact.location")
        .description("Location to store artifacts on a node")
        .defaultOf("/tmp/artifacts/")
        .build();

    static final PropertySpec<String> librarySpec = PropertySpecBuilder.createStr(prefix)
        .name("library.location")
        .description("Location to store service libraries on a node")
        .defaultOf("/tmp/library/")
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(sshPortSpec,
                sshHostSpec,
                userPropertySpec,
                userPasswordSpec,
                artifactSpec)
            .addAll(super.getPropertySpecs())
            .build();
    }

    @Override
    public String name()
    {
        return "SSHOnly";
    }

    @Override
    public String description()
    {
        return "For accessing a single node over ssh";
    }

    @Override
    public boolean disabledWhenShared()
    {
        return true;
    }

    /**
     * Since this is the sshonly provisioner we do no actual provisioning,
     * only making sure the directories we need are there,
     */
    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.AVAILABLE;
    }

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.waitForAllNodes(node -> {
            String userName = userPropertySpec.value(node);
            String host = sshHostSpec.value(node);
            Integer port = sshPortSpec.value(node);
            String password = userPasswordSpec.value(node);

            new SshProvider(node, userName, host, port, null, password, getDefaultSshShell(nodeGroup));

            return createDirectories(node);
        }, "SshOnly Provisioner: startImpl");
    }

    @Override
    protected boolean destroyImpl(NodeGroup nodeGroup)
    {
        return stopImpl(nodeGroup);
    }

    /**
     * Make sure all directories we plan on using for a given nodegroup are created
     */
    private boolean createDirectories(Node node)
    {
        String createDirCmd = String.format("sudo mkdir -m 777 -p %s %s %s",
            node.getRemoteArtifactPath(), node.getRemoteScratchPath(), node.getRemoteLibraryPath());
        return node.waitForSuccess(createDirCmd);
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        if (nodeGroup.allNodesHaveProvider(SshProvider.class))
        {
            return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        }

        return NodeGroup.State.STOPPED;
    }

    /**
     *  Get the path on each node for a given nodeGroup where their artifacts are/should be stored
     * @return
     */
    @Override
    public String getRemoteArtifactPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        String root = System.getProperty(Provisioner.FORCE_ARTIFACTS_DIR, artifactSpec.value(nodeGroup));
        return Paths.get(root, node.map(Node::getFolderName).orElse("")).toString();
    }

    @Override
    public String getRemoteScratchPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        String root = System.getProperty(Provisioner.FORCE_SCRATCH_DIR, "/tmp/scratch");
        return Paths.get(root, node.map(Node::getFolderName).orElse("")).toString();
    }

    @Override
    public String getRemoteLibraryPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        String root = System.getProperty(Provisioner.FORCE_LIBRARY_DIR, librarySpec.value(nodeGroup));
        return Paths.get(root, node.map(Node::getFolderName).orElse("")).toString();
    }

    @Override
    public boolean cleanDirectoriesBeforeTestRun(NodeGroup nodeGroup)
    {
        return nodeGroup.waitForNodeSpecificSuccess(
            node -> String.format("sudo rm -rf %s/* %s/*", node.getRemoteArtifactPath(), node.getRemoteScratchPath()));
    }
}
