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
package com.datastax.fallout.ops.provisioner;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.NodeResponse;

/** This class should disappear once all the tickets referred to in the overridden methods are implemented */
public abstract class NoRemoteAccessProvisioner extends Provisioner
{
    public final String clusterType;

    protected NoRemoteAccessProvisioner(String clusterType)
    {
        this.clusterType = clusterType;
    }

    public class UnsupportedOperationWithContextException extends UnsupportedOperationException
    {
        private UnsupportedOperationWithContextException(String operation, String remedy, String ticket)
        {
            super("There is currently no support for " + operation + " on " + clusterType + " clusters; " +
                remedy + " for this will be in " + ticket);
        }

        private UnsupportedOperationWithContextException(String operation, String ticket)
        {
            this(operation, "support", ticket);
        }
    }

    @Override
    protected NodeResponse executeImpl(Node node, String command)
    {
        throw new UnsupportedOperationWithContextException(
            "directly executing commands", "better error checking", "FAL-1474");
    }

    @Override
    public CompletableFuture<Boolean> get(Node node, String remotePath, Path localPath, boolean deepCopy)
    {
        throw new UnsupportedOperationWithContextException("downloading files", "FAL-1550");
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, Path localPath, String remotePath, boolean deepCopy,
        int permissions)
    {
        throw new UnsupportedOperationWithContextException("uploading files", "FAL-1550");
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, InputStream inputStream, String remotePath, int permissions)
    {
        throw new UnsupportedOperationWithContextException("uploading files", "FAL-1550");
    }

    @Override
    public String getRemoteArtifactPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        throw new UnsupportedOperationWithContextException("remote files access", "FAL-1209");
    }

    @Override
    public String getRemoteScratchPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        throw new UnsupportedOperationWithContextException("remote files access", "FAL-1209");
    }

    @Override
    public String getRemoteLibraryPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        throw new UnsupportedOperationWithContextException("remote files access", "FAL-1209");
    }
}
