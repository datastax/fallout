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
package com.datastax.fallout.ops;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.common.provider.FileProvider.LocalFileProvider;
import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.commands.CommandExecutor;

public class LocalFilesHandler implements HasAvailableProviders
{
    private static final String MANAGED_FILES_DIRECTORY = "managed_files";

    private final List<FileSpec> localFileSpecs;
    private final Path managedFilesPath;
    private final CommandExecutor commandExecutor;

    private LocalFilesHandler(List<FileSpec> localFileSpecs, Path managedFilesPath,
        CommandExecutor commandExecutor)
    {
        this.localFileSpecs = localFileSpecs;
        this.managedFilesPath = managedFilesPath;
        this.commandExecutor = commandExecutor;
    }

    public static LocalFilesHandler fromMaps(List<Map<String, Object>> fileSpecMaps,
        Path testRunArtifactPath, CommandExecutor commandExecutor)
    {
        return new LocalFilesHandler(
            fileSpecMaps.stream().map(FileSpec::fromMap).toList(),
            testRunArtifactPath.resolve(MANAGED_FILES_DIRECTORY),
            commandExecutor);
    }

    public static LocalFilesHandler empty()
    {
        return new LocalFilesHandler(List.of(), null, null);
    }

    public boolean createAllLocalFiles(Ensemble ensemble)
    {
        if (localFileSpecs.isEmpty())
        {
            return true;
        }

        if (!localFileSpecs.stream().allMatch(fileSpec -> fileSpec.createLocalFile(
            ensemble.logger(),
            commandExecutor,
            fileSpec.getFullPath(managedFilesPath))))
        {
            return false;
        }

        ensemble.getUniqueNodeGroupInstances().stream().map(NodeGroup::getNodes).flatMap(List::stream)
            .forEach(node -> new LocalFileProvider(node, managedFilesPath));

        return true;
    }

    /** This is the path to which local copies of files uploaded to remote NodeGroups by {@link
     *  com.datastax.fallout.components.common.configuration_manager.RemoteFilesConfigurationManager} are saved */
    public static Path getRootFileLocation(NodeGroup nodeGroup)
    {
        return nodeGroup.getLocalArtifactPath().resolve(MANAGED_FILES_DIRECTORY);
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders()
    {
        return localFileSpecs.isEmpty() ?
            Set.of() :
            Set.of(FileProvider.LocalFileProvider.class);
    }

    public PropertyRefExpander.Handler createPropertyRefHandler()
    {
        return new PropertyRefExpander.Handler("file") {
            @Override
            public String expandKey(String relativePath)
            {
                if (localFileSpecs.stream().noneMatch(fileSpec -> fileSpec.matchesManagedFileRef(relativePath)))
                {
                    throw new InvalidConfigurationException(String.format(
                        "<<file:%s>> is not defined in a local_files section", relativePath));
                }
                return managedFilesPath.resolve(relativePath).toString();
            }
        };
    }
}
