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
package com.datastax.fallout.ops.provisioner;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.runner.CheckResourcesResult;

/**
 * Simple provisioner for tests and local work.
 *
 * Executes commands locally using ProcessBuilder.
 */
@AutoService(Provisioner.class)
public class LocalProvisioner extends Provisioner implements Provisioner.SingleNodeProvisioner
{
    static final Logger logger = LoggerFactory.getLogger(LocalProvisioner.class);
    static final String prefix = "fallout.provisioner.local.";

    static final PropertySpec<String> artifactSpec = PropertySpecBuilder.createStr(prefix)
        .name("artifact.location")
        .description("Location to store artifacts on this machine")
        .defaultOf("/tmp/artifacts/")
        .build();

    static final PropertySpec<String> librarySpec = PropertySpecBuilder.createStr(prefix)
        .name("library.location")
        .description("Location to store service libraries on a node")
        .defaultOf("/tmp/library/")
        .build();

    private final UUID provisionerID = UUID.randomUUID();

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(artifactSpec)
            .addAll(super.getPropertySpecs())
            .build();
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "Local";
    }

    @Override
    public String description()
    {
        return "For accessing the local node only (no provisioning)";
    }

    @Override
    public boolean disabledWhenShared()
    {
        return true;
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        Set<Class<? extends Provider>> availableProviders = new HashSet<>();

        availableProviders.addAll(super.getAvailableProviders(nodeGroupProperties));

        return availableProviders;
    }

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        //Create needed directories
        return CheckResourcesResult.fromWasSuccessful(createDirectories(nodeGroup));
    }

    /**
     * Make sure all directories we plan on using for a given nodegroup are created
     * @param nodeGroup
     * @return
     */
    private boolean createDirectories(NodeGroup nodeGroup)
    {
        return Stream.concat(
            Stream.of(
                nodeGroup.getRemoteArtifactPath(), nodeGroup.getRemoteScratchPath(), nodeGroup.getRemoteLibraryPath()),
            nodeGroup.getNodes().stream().flatMap(node -> Stream.of(
                node.getRemoteArtifactPath(), node.getRemoteScratchPath(), node.getRemoteLibraryPath())))
            .map(dir -> new File(dir).getAbsoluteFile())
            .noneMatch(f -> !f.exists() && !f.mkdirs());
    }

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
    protected boolean destroyImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        if (nodeGroup.getNodes().stream()
            .allMatch(aNode -> Files.exists(Paths.get(aNode.getRemoteArtifactPath())) &&
                Files.exists(Paths.get(aNode.getRemoteScratchPath()))))
            return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;

        return NodeGroup.State.DESTROYED;
    }

    /**
     *  Get the path on each node for a given nodeGroup where their artifacts are/should be stored
     * @return
     */
    @Override
    public String getRemoteArtifactPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        String root = System.getProperty(Provisioner.FORCE_ARTIFACTS_DIR, artifactSpec.value(nodeGroup));
        return Paths.get(root, provisionerID.toString(), node.map(Node::getFolderName).orElse("")).toString();
    }

    @Override
    public String getRemoteScratchPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        String root = System.getProperty(Provisioner.FORCE_SCRATCH_DIR,
            Paths.get(System.getProperty("java.io.tmpdir")).toString());
        return Paths.get(root, "scratch", provisionerID.toString(), node.map(Node::getFolderName).orElse(""))
            .toString();
    }

    @Override
    public String getRemoteLibraryPath(NodeGroup nodeGroup, Optional<Node> node)
    {
        String root = System.getProperty(Provisioner.FORCE_LIBRARY_DIR, librarySpec.value(nodeGroup));
        return Paths.get(root, provisionerID.toString(), node.map(Node::getFolderName).orElse("")).toString();
    }

    @Override
    public boolean cleanDirectoriesBeforeTestRun(NodeGroup nodeGroup)
    {
        return nodeGroup.getNodes().stream()
            .flatMap(n -> Stream.of(n.getRemoteArtifactPath(), n.getRemoteScratchPath()))
            .map(Paths::get)
            .map(dir -> {
                try
                {
                    MoreFiles.deleteDirectoryContents(dir, RecursiveDeleteOption.ALLOW_INSECURE);
                    return true;
                }
                catch (IOException e)
                {
                    logger.error(e.getMessage());
                    return false;
                }
            })
            .reduce(true, Boolean::logicalAnd);
    }

    @Override
    protected NodeResponse executeImpl(Node node, String command)
    {
        return getCommandExecutor().executeLocally(node, command);
    }

    @Override
    public CompletableFuture<Boolean> get(Node node, String remotePath, Path localPath, boolean deepCopy)
    {
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                File remoteFile = new File(remotePath);
                if (remoteFile.isDirectory())
                {
                    // Create this directory, before filling it
                    File localFile = localPath.toFile();
                    localFile.mkdirs();
                }

                if (deepCopy && remoteFile.isDirectory())
                {
                    AtomicBoolean result = new AtomicBoolean(true);
                    FileUtils.copyDirectory(remoteFile, localPath.toFile(), (file) -> {
                        boolean readable = Files.isReadable(file.toPath());
                        if (!readable)
                        {
                            node.logger().error("Could not copy {} (unreadable); continuing to copy other files...",
                                file);
                            result.set(false);
                        }
                        return readable;
                    });
                    return result.get();
                }
                else if (remoteFile.isDirectory())
                {
                    for (File file : remoteFile.listFiles())
                    {
                        if (!file.isDirectory())
                            FileUtils.copyFile(file, localPath.resolve(file.getName()).toFile());
                    }
                }
                else
                {
                    // Create path to this file, before transferring
                    Files.createDirectories(localPath.getParent());

                    FileUtils.copyFile(remoteFile, localPath.toFile());
                }
            }
            catch (IOException e)
            {
                logger.error(ExceptionUtils.getStackTrace(e));
                return false;
            }
            return true;
        }).exceptionally(e -> {
            logger.error(ExceptionUtils.getStackTrace(e));
            return false;
        });
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, Path localPath, String remotePath, boolean deepCopy,
        int permissions)
    {
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                File localFile = localPath.toFile();
                if (deepCopy && localFile.isDirectory())
                {
                    FileUtils.copyDirectory(localFile, new File(remotePath));
                }
                else if (localFile.isDirectory())
                {
                    Files.createDirectories(Paths.get(remotePath));
                    for (File file : localFile.listFiles())
                    {
                        if (!file.isDirectory())
                            FileUtils.copyFile(file, Paths.get(remotePath, file.getName()).toFile());
                    }
                }
                else
                    FileUtils.copyFile(localFile, new File(remotePath));

                if (permissions > 0)
                {
                    setFilePermissions(remotePath, permissions);
                }
            }
            catch (IOException e)
            {
                logger.error(ExceptionUtils.getStackTrace(e));
                return false;
            }
            return true;
        }).exceptionally(e -> {
            logger.error(ExceptionUtils.getStackTrace(e));
            return false;
        });
    }

    @Override
    public CompletableFuture<Boolean> put(Node node, InputStream inputStream, String remotePath, int permissions)
    {
        return CompletableFuture.supplyAsync(() -> {
            try
            {
                OutputStream outputStream = new FileOutputStream(new File(remotePath));

                byte[] buffer = new byte[1024]; // Adjust if you want
                int bytesRead;

                while ((bytesRead = inputStream.read(buffer)) != -1)
                {
                    outputStream.write(buffer, 0, bytesRead);
                }
                if (permissions > 0)
                {
                    setFilePermissions(remotePath, permissions);
                }
            }
            catch (IOException e)
            {
                logger.error(ExceptionUtils.getStackTrace(e));
                return false;
            }
            return true;
        }).exceptionally(e -> {
            logger.error(ExceptionUtils.getStackTrace(e));
            return false;
        });
    }

    private void setFilePermissions(String path, int permissions) throws IOException
    {
        Runtime.getRuntime().exec(String.format("chmod %o %s", permissions, path));
    }

}
