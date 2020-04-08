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
package com.datastax.fallout.ops.providers;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.LocalFilesHandler;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provider;

public abstract class FileProvider extends Provider
{
    private static final Pattern YAML_FILE_MARKER = Pattern.compile("<<file:/*(.+)>>");

    FileProvider(Node node)
    {
        super(node);
    }

    abstract Path resolveFullFilePath(String extractedPath);

    public static String getRelativePath(String pathInYaml)
    {
        Matcher m = YAML_FILE_MARKER.matcher(pathInYaml);
        if (!m.matches())
        {
            throw new RuntimeException(
                String.format("Given path %s does not match file marker: %s", pathInYaml, YAML_FILE_MARKER));
        }
        return m.group(1);
    }

    public Path getFullPath(String pathInYaml)
    {
        return resolveFullFilePath(getRelativePath(pathInYaml));
    }

    public static boolean isManagedFile(String fromYaml)
    {
        return YAML_FILE_MARKER.matcher(fromYaml).matches();
    }

    public static boolean validateIsManagedFile(Object fromYaml)
    {
        if (!isManagedFile(fromYaml.toString()))
        {
            throw new InvalidConfigurationException(
                String.format("Value must be a managed file, but found %s", fromYaml));
        }
        return true;
    }

    public static void addProviderIfRequired(Set<Class<? extends Provider>> required, Optional<String> path)
    {
        if (path.map(FileProvider::isManagedFile).orElse(false))
        {
            required.add(FileProvider.class);
        }
    }

    public static class LocalFileProvider extends FileProvider
    {
        public LocalFileProvider(Node node)
        {
            super(node);
        }

        @Override
        public String name()
        {
            return "local_file";
        }

        @Override
        Path resolveFullFilePath(String extractedPath)
        {
            return node.getNodeGroup().getLocalArtifactPath()
                .resolve(LocalFilesHandler.MANAGED_FILES_DIRECTORY)
                .resolve(extractedPath);
        }
    }

    public static class RemoteFileProvider extends FileProvider
    {
        public RemoteFileProvider(Node node)
        {
            super(node);
        }

        public static String expandManagedFileSpec(NodeGroup nodeGroup, String file)
        {
            return isManagedFile(file) ?
                nodeGroup.findFirstRequiredProvider(RemoteFileProvider.class)
                    .getFullPath(file).toString() :
                file;
        }

        @Override
        public String name()
        {
            return "remote_file";
        }

        @Override
        Path resolveFullFilePath(String extractedPath)
        {
            return Paths.get(node.getNodeGroup().getRemoteLibraryPath(), extractedPath);
        }
    }
}
