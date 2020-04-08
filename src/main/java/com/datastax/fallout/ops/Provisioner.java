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
package com.datastax.fallout.ops;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.LocalCommandExecutor;
import com.datastax.fallout.ops.commands.NodeCommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.providers.NodeInfoProvider;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;

/**
 * Represents an api that can reserve/provision Nodes.
 *
 * The state transitions and validation checks are managed by this abstract
 * class.  A custom provisioner must implement the actual core api methods
 *
 * All operations must be idempotent
 * All operations are async
 * Operations are synchronized at the nodeGroup/Node level
 */

public abstract class Provisioner extends EnsembleComponent implements NodeCommandExecutor, DebugInfoProvidingComponent
{
    public final static String FORCE_ARTIFACTS_DIR = "fallout.force.artifacts.dir";
    public final static String FORCE_SCRATCH_DIR = "fallout.force.scratch.dir";
    public final static String FORCE_LIBRARY_DIR = "fallout.force.library.dir";

    private String instanceName;
    private CommandExecutor commandExecutor = new LocalCommandExecutor();

    private final PropertySpec<String> nameSpec;

    public Provisioner()
    {
        nameSpec = PropertySpecBuilder.createName(prefix())
            .name("name")
            .description("The name of the cluster to be provisioned")
            .build();
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.of(nameSpec);
    }

    protected Optional<String> explicitClusterName(NodeGroup nodeGroup)
    {
        return nameSpec.optionalValue(nodeGroup);
    }

    public String generateClusterName(NodeGroup nodeGroup, String testRunName, TestRun testRun,
        User user)
    {
        return ClusterNames.generateClusterName(nodeGroup, testRunName, testRun, user);
    }

    public String clusterName(NodeGroup nodeGroup)
    {
        return explicitClusterName(nodeGroup)
            .orElseGet(() -> FalloutPropertySpecs.generatedClusterNamePropertySpec.value(nodeGroup));
    }

    public void setLocalCommandExecutor(CommandExecutor commandExecutor)
    {
        this.commandExecutor = commandExecutor;
    }

    public CommandExecutor getCommandExecutor()
    {
        return commandExecutor;
    }

    @Override
    public void setInstanceName(String modulePhaseName)
    {
        Preconditions.checkArgument(this.instanceName == null, "Provisioner instance name already set");
        this.instanceName = modulePhaseName;
    }

    @Override
    public String getInstanceName()
    {
        return instanceName;
    }

    public Optional<ResourceRequirement> getResourceRequirements(NodeGroup nodeGroup)
    {
        return Optional.empty();
    }

    protected abstract CheckResourcesResult reserveImpl(NodeGroup nodeGroup);

    protected CheckResourcesResult createImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.AVAILABLE;
    }

    protected boolean prepareImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    final public void summarizeInfo(InfoConsumer infoConsumer)
    {
        HashMap<String, Object> info = new HashMap<>();
        doSummarizeInfo(info::put);
        String component_prefix = prefix().replace("fallout.provisioner", "provisioner");
        infoConsumer.accept(component_prefix.substring(0, component_prefix.length() - 1), info);
    }

    /**
     * Left empty so child classes aren't required to implement if they have nothing to add
     * @param infoConsumer
     */
    public void doSummarizeInfo(InfoConsumer infoConsumer)
    {

    }

    protected abstract boolean startImpl(NodeGroup nodeGroup);

    protected abstract boolean stopImpl(NodeGroup nodeGroup);

    @Override
    final public NodeResponse execute(Node node, String command)
    {
        NodeGroup.State state = node.getNodeGroup().getState();
        if (!state.isStarted() && state != NodeGroup.State.STOPPING && state != NodeGroup.State.STARTING &&
            state != NodeGroup.State.CREATED && !state.isUnknownState())
        {
            throw new IllegalStateException(
                String.format("Can't call execute on node when node group is in %s state.", state));
        }

        node.getProperties().validate(getPropertySpecs());

        node.logger().info("Executing on node {}: {}", node.getId(), command);
        return executeImpl(node, command);
    }

    protected abstract NodeResponse executeImpl(Node node, String command);

    protected abstract boolean destroyImpl(NodeGroup nodeGroup);

    /**
     * Download the provisioner specific artifacts, if any
     *
     * @param nodeGroup
     * @param localPath
     * @return
     */
    final public CompletableFuture<Boolean> downloadProvisionerArtifacts(NodeGroup nodeGroup, Path localPath)
    {
        return CompletableFuture.supplyAsync(() -> downloadProvisionerArtifactsImpl(nodeGroup, localPath));
    }

    /**
     * Copy all provisioner-specific artifacts in {@code localPath}, if there is any
     */
    protected boolean downloadProvisionerArtifactsImpl(NodeGroup nodeGroup, Path localPath)
    {
        return true;
    }

    public CompletableFuture<Boolean> downloadProvisionerArtifacts(Node node, Path localPath)
    {
        return CompletableFuture.completedFuture(true);
    }

    /**
     * Returns the providers added to each Node by this Provisioner for the given NodeGroup properties
     *
     * @see Module#getRequiredProviders()
     *
     * @param nodeGroupProperties
     * @return the set of Providers to be installed on each Node
     */
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return ImmutableSet.of(NodeInfoProvider.class);
    }

    /**
     * Inspects the node when its current state is unknown.  If a sensible state can't be derived,
     * returns the current state.
     */
    public final CompletableFuture<NodeGroup.State> checkState(NodeGroup nodeGroup)
    {
        return CompletableFuture.supplyAsync(() -> checkStateImpl(nodeGroup));
    }

    /** Override in subclasses if they can derive the node state */
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.getState();
    }

    public abstract String getRemoteArtifactPath(NodeGroup nodeGroup, Optional<Node> node);

    public abstract String getRemoteScratchPath(NodeGroup nodeGroup, Optional<Node> node);

    public abstract String getRemoteLibraryPath(NodeGroup nodeGroup, Optional<Node> node);

    /** Removes contents of remote artifact and scratch directories */
    public boolean cleanDirectoriesBeforeTestRun(NodeGroup nodeGroup)
    {
        return true;
    }

    /**
     * Marker interface for provisioners that only reserve
     * or use a single machine/container.
     *
     * @see com.datastax.fallout.ops.provisioner.LocalProvisioner
     */
    public interface SingleNodeProvisioner
    {
    }

    /**
     * Transfers file or directory from remotePath at node, to localPath. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param node Node to transfer from
     * @param remotePath Path on remote node to transfer, can be file or dir
     * @param localPath Path to transfer to
     * @param deepCopy To recursively copy subdirs, if remotePath is a directory
     * @return
     */
    public abstract CompletableFuture<Boolean> get(Node node, String remotePath, Path localPath, boolean deepCopy);

    /**
     * Transfers file or directory from localPath. to remotePath on node. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param node Node to transfer to
     * @param localPath Path to transfer from, can be file or dir
     * @param remotePath Path on remote node to transfer to
     * @param deepCopy To recursively copy subdirs, if localPath is a directory
     * @return
     */
    public final CompletableFuture<Boolean> put(Node node, Path localPath, String remotePath, boolean deepCopy)
    {
        return put(node, localPath, remotePath, deepCopy, 0);
    }

    /**
     * Transfers file or directory from localPath. to remotePath on node. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param node Node to transfer to
     * @param localPath Path to transfer from, can be file or dir
     * @param remotePath Path on remote node to transfer to
     * @param deepCopy To recursively copy subdirs, if localPath is a directory
     * @param permissions The file permissions to set on the remote node (0 = set no permissions)
     * @return
     */
    public abstract CompletableFuture<Boolean> put(Node node, Path localPath, String remotePath, boolean deepCopy,
        int permissions);

    /**
     * Transfers inputStream to remotePath on node. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param node Node to transfer from
     * @param inputStream Input Stream to transfer
     * @param remotePath Path on remote node to transfer to
     * @param permissions The file permissions to set on the remote node (0 = set no permissions)
     * @return
     */
    public abstract CompletableFuture<Boolean> put(Node node, InputStream inputStream, String remotePath,
        int permissions);

    /**
     * Gets the contents of a java package specific resource
     * @param resourceName
     * @return
     */
    protected Optional<byte[]> getResource(String resourceName)
    {
        return Utils.getResource(this, resourceName);
    }
}
