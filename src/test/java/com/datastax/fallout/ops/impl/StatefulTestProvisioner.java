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
package com.datastax.fallout.ops.impl;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.runner.CheckResourcesResult;

/**
 * Stateful test provisioner used to inject failures
 */
public class StatefulTestProvisioner extends Provisioner
{
    private boolean failTransitions = false;
    private Optional<NodeGroup.State> forcedCheckState = Optional.empty();

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.fromWasSuccessful(!failTransitions);
    }

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        return !failTransitions;
    }

    @Override
    protected boolean stopImpl(NodeGroup nodeGroup)
    {
        return !failTransitions;
    }

    @Override
    protected NodeResponse executeImpl(Node node, String command)
    {
        return new NodeResponse.ErrorResponse(node, "Unsupported");
    }

    @Override
    protected boolean destroyImpl(NodeGroup nodeGroup)
    {
        return !failTransitions;
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        NodeGroup.State state;

        if (forcedCheckState.isPresent())
        {
            return forcedCheckState.get();
        }
        else
        {
            state =
                nodeGroup.getState().equals(NodeGroup.State.UNKNOWN) ? NodeGroup.State.DESTROYED : nodeGroup.getState();
            return state;
        }
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
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, Path localPath, String remotePath, boolean deepCopy,
        int permissions)
    {
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, InputStream inputStream, String remotePath, int permissions)
    {
        return CompletableFuture.completedFuture(false);
    }

    @Override
    public String prefix()
    {
        return "fallout.provisioner.test.";
    }

    @Override
    public String name()
    {
        return "statefulTest";
    }

    @Override
    public String description()
    {
        return "Stateful test provisioner";
    }

    public void setCheckState(NodeGroup.State state)
    {
        forcedCheckState = Optional.of(state);
    }

    public void clearCheckState()
    {
        forcedCheckState = Optional.empty();
    }

    public void failTransitions()
    {
        failTransitions = true;
    }

    public void goodTransitions()
    {
        failTransitions = false;
    }
}
