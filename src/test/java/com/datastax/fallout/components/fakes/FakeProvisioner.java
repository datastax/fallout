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
package com.datastax.fallout.components.fakes;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.auto.service.AutoService;

import com.datastax.fallout.components.common.provider.NodeInfoProvider;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.runner.CheckResourcesResult;

@AutoService(Provisioner.class)
public class FakeProvisioner extends Provisioner
{
    protected static final String NAME = "fake";
    protected static final String PREFIX = "test.provisioner." + NAME + ".";

    @Override
    public String prefix()
    {
        return PREFIX;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public String description()
    {
        return "Fake provisioner";
    }

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.AVAILABLE;
    }

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        nodeGroup.getNodes().forEach(node -> new NodeInfoProvider(
            node, "127.0.0.1", "127.0.0.1", "mrfakey"));
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
        return new NodeResponse.ErrorResponse(node, "Unsupported");
    }

    @Override
    protected boolean destroyImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.getState().isUnknownState() ? NodeGroup.State.DESTROYED : nodeGroup.getState();
    }

    @Override
    public String getRemoteArtifactPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        return "/tmp";
    }

    @Override
    public String getRemoteScratchPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        return "/tmp";
    }

    @Override
    public String getRemoteLibraryPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        return "/tmp";
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

}
