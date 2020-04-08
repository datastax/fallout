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
package com.datastax.fallout.ops.configmanagement;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.FileSpec;
import com.datastax.fallout.ops.LocalFilesHandler;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.providers.FileProvider;

@AutoService(ConfigurationManager.class)
public class RemoteFilesConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.remote_files.";

    private final PropertySpec<List<FileSpec>> filesSpec = PropertySpecBuilder.create(prefix)
        .name("files")
        .description("List of remote file spec definitions. See example usage.")
        .parser(RemoteFilesConfigurationManager::fileSpecParser)
        .required()
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "remote_files";
    }

    @Override
    public String description()
    {
        return "Downloads or creates files to include with a test run on remote nodes.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("managed-files");
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.of(filesSpec);
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        List<FileSpec> fileSpecs = filesSpec.value(properties);
        Set<String> seenPaths = new HashSet<>();
        Set<String> duplicatePaths = new HashSet<>();

        for (FileSpec fileSpec : fileSpecs)
        {
            if (!seenPaths.add(fileSpec.getPath()))
            {
                duplicatePaths.add(fileSpec.getPath());
            }
        }
        if (!duplicatePaths.isEmpty())
        {
            throw new InvalidConfigurationException(String.format("Some file spec paths are not unique: %s",
                duplicatePaths));
        }
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup properties)
    {
        return ImmutableSet.of(FileProvider.RemoteFileProvider.class);
    }

    @Override
    public boolean registerProviders(Node node)
    {
        new FileProvider.RemoteFileProvider(node);
        return true;
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        List<CompletableFuture<Boolean>> futures = fileSpecsStream(nodeGroup)
            .map(fileSpec -> CompletableFuture.supplyAsync(() -> this.createFile(nodeGroup, fileSpec)))
            .collect(Collectors.toList());
        return Utils.waitForAll(futures, nodeGroup.logger(), "Creating files");
    }

    @Override
    public boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return fileSpecsStream(nodeGroup)
            .map(fileSpec -> this.deleteFile(nodeGroup, fileSpec))
            .reduce(Boolean::logicalAnd)
            .orElse(false);
    }

    @Override
    public NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        boolean filesExistWithLocalCopy = fileSpecsStream(nodeGroup)
            .map(fileSpec -> fileExists(nodeGroup, fileSpec) && saveLocalCopy(nodeGroup, fileSpec))
            .reduce(Boolean::logicalAnd)
            .orElse(false);

        return filesExistWithLocalCopy ?
            NodeGroup.State.STARTED_SERVICES_RUNNING :
            NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    private Stream<FileSpec> fileSpecsStream(NodeGroup nodeGroup)
    {
        return filesSpec.value(nodeGroup).stream();
    }

    private static List<FileSpec> fileSpecParser(Object o)
    {
        List<Map<String, Object>> l = (List<Map<String, Object>>) o;
        return l.stream()
            .map(FileSpec::fromMap)
            .collect(Collectors.toList());
    }

    private Path getRootFileLocation(NodeGroup nodeGroup)
    {
        return Paths.get(nodeGroup.getRemoteLibraryPath());
    }

    private boolean fileExists(NodeGroup nodeGroup, FileSpec fileSpec)
    {
        Path expectedFile = fileSpec.getFullPath(getRootFileLocation(nodeGroup));
        return nodeGroup.waitForAllNodes(
            n -> n.executeExistsCheck(fileSpec.getExistsCheckType(), expectedFile.toString()),
            String.format("Checking file %s exists", expectedFile));
    }

    private boolean createFile(NodeGroup nodeGroup, FileSpec fileSpec)
    {
        Path fullFilePath = fileSpec.getFullPath(getRootFileLocation(nodeGroup));
        boolean pathToFileExists = nodeGroup.waitForAllNodes(n -> n.existsDir(fullFilePath.getParent().toString()),
            "checking remote path to file exists");

        if (!pathToFileExists)
        {
            boolean pathToFileCreated = nodeGroup.waitForSuccess(
                String.format("mkdir -p %s", fullFilePath.getParent()));
            if (!pathToFileCreated)
            {
                nodeGroup.logger().error("Could not create path to file!");
                return false;
            }
        }

        nodeGroup.logger().info("Attempting to create file at {} ", fullFilePath);
        Path localFilePath = fileSpec.getFullPath(LocalFilesHandler.getRootFileLocation(nodeGroup));
        return fileSpec.createRemoteAndShadowFile(fullFilePath, localFilePath, nodeGroup).join();
    }

    private boolean saveLocalCopy(NodeGroup nodeGroup, FileSpec fileSpec)
    {
        Path remoteFilePath = fileSpec.getFullPath(getRootFileLocation(nodeGroup));
        Path localFilePath = fileSpec.getFullPath(LocalFilesHandler.getRootFileLocation(nodeGroup));
        return fileSpec.createShadowFile(remoteFilePath, localFilePath, nodeGroup).join();
    }

    private boolean deleteFile(NodeGroup nodeGroup, FileSpec fileSpec)
    {
        return nodeGroup.waitForSuccess(String.format("rm -rf %s",
            fileSpec.getFullPath(getRootFileLocation(nodeGroup))));
    }
}
