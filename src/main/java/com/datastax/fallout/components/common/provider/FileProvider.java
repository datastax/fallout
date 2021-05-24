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
package com.datastax.fallout.components.common.provider;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import com.datastax.fallout.components.common.configuration_manager.RemoteFilesConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Provider;

public abstract class FileProvider extends Provider
{
    private static final Pattern MANAGED_FILE_REF_PATTERN = Pattern.compile("<<file:([^>]+)>>");

    FileProvider(Node node)
    {
        super(node);
    }

    abstract Path getFullPath(String managedFileRef);

    public static String getRelativePath(String managedFileRef)
    {
        Matcher m = MANAGED_FILE_REF_PATTERN.matcher(managedFileRef);
        if (!m.matches())
        {
            throw new RuntimeException(
                String.format("Given managed file ref %s does not match <<file: ... >>", managedFileRef));
        }
        return m.group(1);
    }

    public static boolean isManagedFileRef(String managedFileRef)
    {
        return MANAGED_FILE_REF_PATTERN.matcher(managedFileRef).matches();
    }

    public static void addProviderIfRequired(Set<Class<? extends Provider>> required, Optional<String> path)
    {
        if (path.map(FileProvider::isManagedFileRef).orElse(false))
        {
            required.add(FileProvider.class);
        }
    }

    public static String expandManagedFileRefs(String str, Function<String, String> getAbsolutePath)
    {
        final var matcher = MANAGED_FILE_REF_PATTERN.matcher(str);
        return matcher.replaceAll(matchResult -> getAbsolutePath.apply(matchResult.group(1)).toString());
    }

    public static class LocalFileProvider extends FileProvider
    {
        private final Path managedFilesPath;

        public LocalFileProvider(Node node, Path managedFilesPath)
        {
            super(node);
            this.managedFilesPath = managedFilesPath;
        }

        @Override
        public String name()
        {
            return "local_file";
        }

        @Override
        public Path getFullPath(String managedFileRef)
        {
            return managedFilesPath.resolve(getRelativePath(managedFileRef));
        }
    }

    public static class RemoteFileProvider extends FileProvider
    {
        private final RemoteFilesConfigurationManager.RemoteFileHandler handler;

        public RemoteFileProvider(Node node, RemoteFilesConfigurationManager.RemoteFileHandler handler)
        {
            super(node);
            this.handler = handler;
        }

        public static String expandManagedFileRef(NodeGroup nodeGroup, String managedFileRef)
        {
            return isManagedFileRef(managedFileRef) ?
                nodeGroup.findFirstRequiredProvider(RemoteFileProvider.class).getFullPath(managedFileRef).toString() :
                managedFileRef;
        }

        public String expandRefs(String value)
        {
            return handler.expandRefs(value);
        }

        @Override
        public String name()
        {
            return "remote_file";
        }

        /**
         * The location where the contents of the file can be found.
         * For SSH clusters, this is a absolute path.
         * For Kubernetes clusters, this is the expected mount point within a container.
         */
        @Override
        public Path getFullPath(String managedFileRef)
        {
            return handler.getFilePath(getRelativePath(managedFileRef));
        }

        public String getConfigMapName(String managedFileRef)
        {
            return handler.getConfigMapName(getRelativePath(managedFileRef));
        }
    }

    public static abstract class ManagedFileRef
    {
        final String managedFileRef;
        private final Class<? extends FileProvider> providerClazz;

        public ManagedFileRef(String managedFileRef, Class<? extends FileProvider> providerClazz)
        {
            Preconditions.checkNotNull(managedFileRef);
            Preconditions.checkArgument(isManagedFileRef(managedFileRef),
                "managed file refs must match the regular expression '" + MANAGED_FILE_REF_PATTERN + "'");
            this.managedFileRef = managedFileRef;
            Preconditions.checkNotNull(providerClazz);
            this.providerClazz = providerClazz;
        }

        public Path fullPath(NodeGroup nodeGroup)
        {
            return nodeGroup.findFirstRequiredProvider(providerClazz).getFullPath(managedFileRef);
        }
    }

    public static class LocalManagedFileRef extends ManagedFileRef
    {
        public LocalManagedFileRef(String managedFileRef)
        {
            super(managedFileRef, LocalFileProvider.class);
        }
    }

    public static class RemoteManagedFileRef extends ManagedFileRef
    {
        public RemoteManagedFileRef(String managedFileRef)
        {
            super(managedFileRef, RemoteFileProvider.class);
        }

        public String fileName(NodeGroup nodeGroup)
        {
            return nodeGroup.findFirstRequiredProvider(RemoteFileProvider.class).getConfigMapName(managedFileRef);
        }
    }
}
