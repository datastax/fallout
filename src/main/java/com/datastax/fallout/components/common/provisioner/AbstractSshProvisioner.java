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

import java.io.File;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.slf4j.Logger;

import com.datastax.fallout.components.common.provider.SshProvider;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.FileUtils;

import static com.datastax.fallout.components.common.provider.SshProvider.SSH_CONNECT_TIMEOUT;
import static com.jcraft.jsch.ChannelSftp.SSH_FX_PERMISSION_DENIED;

public abstract class AbstractSshProvisioner extends Provisioner
{
    private static final int CHANNEL_CONNECT_RETRIES = 5;

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        Set<Class<? extends Provider>> providers = new HashSet<>();
        providers.addAll(super.getAvailableProviders(nodeGroupProperties));
        providers.add(SshProvider.class);
        return providers;
    }

    @Override
    protected NodeResponse executeImpl(Node node, String command)
    {
        return node.getProvider(SshProvider.class).execute(command);
    }

    @Override
    protected boolean stopImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.waitForAllNodes(node -> {
            node.maybeUnregister(SshProvider.class);
            return true;
        }, "AbstractSSH Provisioner: disconnecting any present sessions");
    }

    protected interface SftpOperation
    {
        boolean exec(ChannelSftp sftpChannel) throws SftpException;
    }

    public static void connectToChannelWithRetries(Channel channel, Logger logger) throws JSchException
    {
        JSchException exception = null;
        int attempts = 0;
        while (attempts < CHANNEL_CONNECT_RETRIES)
        {
            try
            {
                channel.connect(SSH_CONNECT_TIMEOUT);
                return;
            }
            catch (JSchException e)
            {
                logger.warn(
                    String.format("JSchException when trying to connect to channel. Attempt %d of %d.",
                        attempts + 1, CHANNEL_CONNECT_RETRIES),
                    e);
                exception = e;
                attempts++;
            }
        }
        throw exception;
    }

    protected CompletableFuture<Boolean> withSftpChannel(Node node, SftpOperation operation)
    {
        return CompletableFuture.supplyAsync(
            () -> {
                ChannelSftp sftpChannel = null;
                boolean result = true;
                try
                {
                    Session session = node.getProvider(SshProvider.class).getConnectedSession();
                    sftpChannel = (ChannelSftp) session.openChannel("sftp");
                    if (sftpChannel == null)
                    {
                        node.logger().error("Failed to create sftp channel");
                        return false;
                    }
                    connectToChannelWithRetries(sftpChannel, node.logger());
                    result = operation.exec(sftpChannel);
                }
                catch (Throwable e)
                {
                    node.logger().error("sftp operation failed", e);
                    return false;
                }
                finally
                {
                    if (sftpChannel != null)
                    {
                        sftpChannel.disconnect();
                    }
                }
                return result;
            }).exceptionally(
                e -> {
                    node.logger().error("sftp operation failed", e);
                    return false;
                });
    }

    @Override
    public CompletableFuture<Boolean> get(Node node, String remotePath, Path localPath, boolean deepCopy)
    {
        return withSftpChannel(node, (sftpChannel) -> {
            node.logger().debug(String.format("Copying %s to %s", remotePath, localPath));
            return getRecursively(sftpChannel, remotePath, localPath, deepCopy, node.logger());
        }
        );
    }

    private static boolean getRecursively(ChannelSftp sftpChannel, String remotePath, Path localPath, boolean deepCopy,
        Logger logger) throws SftpException
    {
        boolean result = true;

        SftpATTRS attrs = sftpChannel.lstat(remotePath);
        // If remotePath is just a file, copy it over
        if (!attrs.isDir())
        {
            // Create the path to the local file
            File localFile = localPath.getParent().toFile();
            localFile.mkdirs();

            try
            {
                sftpChannel.get(remotePath, localPath.toString());
            }
            catch (SftpException e)
            {
                if (e.id == SSH_FX_PERMISSION_DENIED)
                {
                    logger.error("Could not download {} ({}); continuing to download other files...",
                        remotePath, e.getMessage());
                    result = false;
                }
                else
                {
                    throw e;
                }
            }
        }
        else
        {
            // If we're a directory, don't just create our parent.
            File localFile = localPath.toFile();
            localFile.mkdirs();

            // if deepCopy is false, get all files
            if (!deepCopy)
            {
                Vector<ChannelSftp.LsEntry> lsEntries = sftpChannel.ls(remotePath);
                for (ChannelSftp.LsEntry lsEntry : lsEntries)
                {
                    if (lsEntry.getAttrs().isDir())
                    {
                        continue;
                    }
                    sftpChannel.get(Paths.get(remotePath, lsEntry.getFilename()).toString(),
                        localPath.resolve(lsEntry.getFilename()).toString());
                }
            }
            // if deepCopy is true, recursively get all subfolders
            else
            {
                Vector<ChannelSftp.LsEntry> lsEntries = sftpChannel.ls(remotePath);
                for (ChannelSftp.LsEntry lsEntry : lsEntries)
                {
                    if (lsEntry.getFilename().equals(".") || lsEntry.getFilename().equals(".."))
                    {
                        continue;
                    }

                    if (lsEntry.getAttrs().isDir())
                    {
                        FileUtils.createDirs(localPath.resolve(lsEntry.getFilename()));
                    }

                    result = getRecursively(sftpChannel, Paths.get(remotePath, lsEntry.getFilename()).toString(),
                        localPath.resolve(lsEntry.getFilename()), deepCopy, logger) && result;
                }
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, Path localPath, String remotePath, boolean deepCopy,
        int permissions)
    {
        return withSftpChannel(node, (sftpChannel) -> {
            node.logger().debug(String.format("Copying %s to %s", localPath, remotePath));
            putRecursively(sftpChannel, localPath, remotePath, deepCopy, permissions);
            return true;
        });
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, InputStream inputStream, String remotePath, int permissions)
    {
        return withSftpChannel(node, (sftpChannel) -> {
            node.logger().debug(String.format("Copying %s to %s", inputStream, remotePath));
            sftpChannel.put(inputStream, remotePath);
            if (permissions > 0)
                sftpChannel.chmod(permissions, remotePath);
            return true;
        });
    }

    private static void putRecursively(ChannelSftp sftpChannel, Path localPath, String remotePath, boolean deepCopy,
        int permissions)
        throws SftpException
    {
        // If localPath is just a file, copy it over
        File localFile = localPath.toFile();
        if (!localFile.isDirectory())
        {
            sftpChannel.put(localPath.toString(), remotePath);
            if (permissions > 0)
                sftpChannel.chmod(permissions, remotePath);
        }
        else
        {
            // If the remote directory doesn't exist, create it
            try
            {
                sftpChannel.lstat(remotePath);
            }
            catch (SftpException e)
            {
                sftpChannel.mkdir(remotePath);
            }
            // If localPath is a dir, and deepCopy is false, transfer all files inside
            if (!deepCopy)
            {
                File[] localDirContents = localFile.listFiles();
                for (File file : localDirContents)
                {
                    if (!file.isDirectory())
                        sftpChannel.put(file.getAbsolutePath(),
                            Paths.get(remotePath, file.getName()).toString(), ChannelSftp.OVERWRITE);
                    if (permissions > 0)
                        sftpChannel.chmod(permissions, Paths.get(remotePath, file.getName()).toString());
                }
            }
            // If deepCopy is true, recursively copy all subfolders
            else
            {
                File[] localDirContents = localFile.listFiles();
                for (File file : localDirContents)
                {
                    String remotePathStr = Paths.get(remotePath, file.getName()).toString();
                    if (file.isDirectory())
                    {
                        try
                        {
                            sftpChannel.lstat(remotePath);
                        }
                        catch (SftpException e)
                        {
                            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE)
                            {
                                sftpChannel.mkdir(remotePathStr);
                            }
                        }
                    }

                    putRecursively(sftpChannel, file.toPath(),
                        Paths.get(remotePath, file.getName()).toString(), deepCopy, permissions);
                }
            }
        }
    }

    protected SshProvider.SshShell getDefaultSshShell(NodeGroup nodeGroup)
    {
        return SshProvider.SshShell.BASH;
    }
}
