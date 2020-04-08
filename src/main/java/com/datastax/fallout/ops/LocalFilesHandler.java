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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.fallout.ops.providers.FileProvider;

public class LocalFilesHandler
{
    public static final String MANAGED_FILES_DIRECTORY = "managed_files";

    private final Set<FileSpec> localFileSpecs;

    public LocalFilesHandler(List<Map<String, Object>> localFileSpecMaps)
    {
        this.localFileSpecs = localFileSpecMaps.stream()
            .map(FileSpec::fromMap)
            .collect(Collectors.toSet());
    }

    public boolean createAllLocalFiles(NodeGroup nodeGroup)
    {
        return localFileSpecs.stream()
            .allMatch(fileSpec -> fileExists(nodeGroup, fileSpec) || createFile(nodeGroup, fileSpec)) &&
            nodeGroup.getNodes().stream()
                .map(n -> {
                    new FileProvider.LocalFileProvider(n);
                    return true;
                })
                .reduce(true, Boolean::logicalAnd);
    }

    private boolean createFile(NodeGroup nodeGroup, FileSpec fileSpec)
    {
        Path fullFilePath = fileSpec.getFullPath(getRootFileLocation(nodeGroup));
        nodeGroup.logger().info("Attempting to create file at {} ", fullFilePath);
        try
        {
            return fileSpec.createLocalFile(fullFilePath, nodeGroup);
        }
        catch (IOException e)
        {
            nodeGroup.logger().error(String.format("Error while creating local file %s", fullFilePath), e);
            return false;
        }
    }

    private boolean fileExists(NodeGroup nodeGroup, FileSpec fileSpec)
    {
        return Files.exists(fileSpec.getFullPath(getRootFileLocation(nodeGroup)));
    }

    public static Path getRootFileLocation(NodeGroup nodeGroup)
    {
        return nodeGroup.getLocalArtifactPath().resolve(MANAGED_FILES_DIRECTORY);
    }

    public Set<Class<? extends Provider>> getAvailableProviders()
    {
        return localFileSpecs.isEmpty() ?
            Set.of() :
            Set.of(FileProvider.LocalFileProvider.class);
    }
}
