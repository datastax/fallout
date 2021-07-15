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
package com.datastax.fallout.components.common.configuration_manager;

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

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.kubernetes.KubeControlProvider;
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
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.components.kubernetes.AbstractKubernetesProvisioner.DNS1123;

@AutoService(ConfigurationManager.class)
public class RemoteFilesConfigurationManager extends ConfigurationManager
{
    private static final String PREFIX = "fallout.configuration.management.remote_files.";
    private static final String NAMESPACE_ARG = "namespace";

    private final PropertySpec<List<RemoteFileSpec>> filesSpec = PropertySpecBuilder
        .<List<RemoteFileSpec>>create(PREFIX)
        .name("files")
        .description("List of remote file spec definitions. See example usage.")
        .parser(RemoteFilesConfigurationManager::remoteFileSpecParser)
        .required()
        .build();

    public static class RemoteFileSpec
    {
        public final FileSpec fileSpec;
        public final Optional<String> namespace;

        private RemoteFileSpec(FileSpec fileSpec, Optional<String> namespace)
        {
            this.fileSpec = fileSpec;
            this.namespace = namespace;
        }

        public Path getLocalFilePath(NodeGroup nodeGroup)
        {
            return fileSpec.getFullPath(LocalFilesHandler.getRootFileLocation(nodeGroup));
        }
    }

    @Override
    public String prefix()
    {
        return PREFIX;
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
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(filesSpec);
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        List<FileSpec> fileSpecs = remoteFileSpecStream(properties)
            .peek(rfs -> handler().validate(rfs))
            .map(rfs -> rfs.fileSpec)
            .collect(Collectors.toList());

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
        return Set.of(FileProvider.RemoteFileProvider.class);
    }

    @Override
    public boolean registerProviders(Node node)
    {
        new FileProvider.RemoteFileProvider(node, handler());
        return true;
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        List<CompletableFuture<Boolean>> futures = remoteFileSpecStream(nodeGroup)
            .map(rfs -> CompletableFuture.supplyAsync(() -> this.createFile(nodeGroup, rfs)))
            .collect(Collectors.toList());
        return Utils.waitForAll(futures, nodeGroup.logger(), "Creating files");
    }

    @Override
    public boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return remoteFileSpecStream(nodeGroup)
            .map(rfs -> handler().deleteRemoteFile(rfs))
            .reduce(Boolean::logicalAnd)
            .orElse(false);
    }

    @Override
    public NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        boolean filesExistWithLocalCopy = remoteFileSpecStream(nodeGroup)
            .map(rfs -> handler().remoteFileExists(rfs) && saveShadedCopy(nodeGroup, rfs.fileSpec))
            .reduce(Boolean::logicalAnd)
            .orElse(false);

        // TODO: FAL-970 return CONFIGURED
        return filesExistWithLocalCopy ?
            NodeGroup.State.STARTED_SERVICES_RUNNING :
            NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    private Stream<RemoteFileSpec> remoteFileSpecStream(NodeGroup nodeGroup)
    {
        return remoteFileSpecStream(nodeGroup.getProperties());
    }

    private Stream<RemoteFileSpec> remoteFileSpecStream(PropertyGroup properties)
    {
        return filesSpec.value(properties).stream();
    }

    @SuppressWarnings("unchecked")
    private static List<RemoteFileSpec> remoteFileSpecParser(Object o)
    {
        List<Map<String, Object>> l = (List<Map<String, Object>>) o;
        return l.stream()
            .map(m -> {
                Optional<String> namespace = Optional.empty();
                if (m.containsKey(NAMESPACE_ARG))
                {
                    namespace = Optional.of((String) m.get(NAMESPACE_ARG));
                    m.remove(NAMESPACE_ARG);
                }
                return new RemoteFileSpec(FileSpec.fromMap(m), namespace);
            })
            .collect(Collectors.toList());
    }

    private boolean createFile(NodeGroup nodeGroup, RemoteFileSpec remoteFileSpec)
    {
        FileSpec fileSpec = remoteFileSpec.fileSpec;
        String path = fileSpec.getPath();

        Path localFilePath = fileSpec.getFullPath(LocalFilesHandler.getRootFileLocation(nodeGroup));

        nodeGroup.logger().info("Attempting to create file {} ", path);
        if (!createLocalCopy(nodeGroup, fileSpec, localFilePath))
        {
            nodeGroup.logger().error("Failed to create local copy of remote file: {}", path);
            return false;
        }
        if (!handler().createRemoteFile(remoteFileSpec))
        {
            nodeGroup.logger().error("Failed to upload remote file: {}", path);
        }
        if (!fileSpec.shadeLocalFile(nodeGroup, localFilePath))
        {
            nodeGroup.logger().error("Failed to shade local copy of remote file: {}", path);
            return false;
        }
        return true;
    }

    private boolean createLocalCopy(NodeGroup nodeGroup, FileSpec fileSpec, Path localFilePath)
    {
        return Exceptions.getUncheckedIO(() -> fileSpec.createLocalFile(
            nodeGroup.logger(), nodeGroup.getProvisioner().getCommandExecutor(), localFilePath));
    }

    private boolean saveShadedCopy(NodeGroup nodeGroup, FileSpec fileSpec)
    {
        Path localFilePath = fileSpec.getFullPath(LocalFilesHandler.getRootFileLocation(nodeGroup));
        return createLocalCopy(nodeGroup, fileSpec, localFilePath) && fileSpec.shadeLocalFile(nodeGroup, localFilePath);
    }

    private RemoteFileHandler handler()
    {
        List<RemoteFileSpec> remoteFileSpecs = remoteFileSpecStream(getNodeGroup()).collect(Collectors.toList());
        return getNodeGroup().willHaveProvider(KubeControlProvider.class) ?
            new KubernetesRemoteFileHandler(remoteFileSpecs) :
            new SshRemoteFileHandler(remoteFileSpecs);
    }

    public abstract class RemoteFileHandler
    {
        private final List<RemoteFileSpec> remoteFileSpecs;

        protected RemoteFileHandler(List<RemoteFileSpec> remoteFileSpecs)
        {
            this.remoteFileSpecs = remoteFileSpecs;
        }

        public RemoteFileSpec getReferencedSpec(String relativePath)
        {
            return remoteFileSpecs.stream()
                .filter(rfs -> rfs.fileSpec.matchesManagedFileRef(relativePath))
                .findFirst()
                .orElseThrow(() -> new RuntimeException(
                    String.format("File reference <<file:%s>> does not match any managed files", relativePath)));
        }

        public String expandRefs(String value)
        {
            return FileProvider.expandManagedFileRefs(value, relativePath -> {
                if (remoteFileSpecs.stream().noneMatch(rfs -> rfs.fileSpec.matchesManagedFileRef(relativePath)))
                {
                    throw new RuntimeException(String.format(
                        "<<file:%s>> is not defined in remote_files configuration manager for node group %s",
                        relativePath, getNodeGroup().getName()));
                }
                return getFilePath(relativePath).toString();
            });
        }

        void validate(RemoteFileSpec rfs)
        {
        }

        public abstract Path getFilePath(String path);

        public abstract String getConfigMapName(String path);

        abstract boolean createRemoteFile(RemoteFileSpec rfs);

        abstract boolean deleteRemoteFile(RemoteFileSpec rfs);

        abstract boolean remoteFileExists(RemoteFileSpec rfs);
    }

    private class SshRemoteFileHandler extends RemoteFileHandler
    {
        private SshRemoteFileHandler(List<RemoteFileSpec> remoteFileSpecs)
        {
            super(remoteFileSpecs);
        }

        private Path getRemoteFilePath(RemoteFileSpec rfs)
        {
            return rfs.fileSpec.getFullPath(Paths.get(getNodeGroup().getRemoteLibraryPath()));
        }

        @Override
        public Path getFilePath(String path)
        {
            return Paths.get(getNodeGroup().getRemoteLibraryPath(), path);
        }

        @Override
        public String getConfigMapName(String path)
        {
            throw new UnsupportedOperationException("Cannot create ConfigMap on an SSH cluster");
        }

        @Override
        public boolean createRemoteFile(RemoteFileSpec rfs)
        {
            Path remoteFilePath = getRemoteFilePath(rfs);
            boolean pathToFileExists =
                getNodeGroup().waitForAllNodes(n -> n.existsDir(remoteFilePath.getParent().toString()),
                    "checking remote path to file exists");
            if (!pathToFileExists)
            {
                boolean pathToFileCreated = getNodeGroup().waitForSuccess(
                    String.format("mkdir -p %s", remoteFilePath.getParent()));
                if (!pathToFileCreated)
                {
                    getNodeGroup().logger().error("Could not create path to file!");
                    return false;
                }
            }
            return getNodeGroup().put(rfs.getLocalFilePath(getNodeGroup()), remoteFilePath.toString(), true).join();
        }

        @Override
        public boolean deleteRemoteFile(RemoteFileSpec rfs)
        {
            return getNodeGroup().waitForSuccess(String.format("rm -rf %s", getRemoteFilePath(rfs)));
        }

        @Override
        public boolean remoteFileExists(RemoteFileSpec rfs)
        {
            Path remoteFilePath = getRemoteFilePath(rfs);
            return getNodeGroup().waitForAllNodes(
                n -> n.executeExistsCheck(rfs.fileSpec.getExistsCheckType(), remoteFilePath.toString()),
                String.format("Checking file %s exists", remoteFilePath));
        }
    }

    private class KubernetesRemoteFileHandler extends RemoteFileHandler
    {
        private KubernetesRemoteFileHandler(List<RemoteFileSpec> remoteFileSpecs)
        {
            super(remoteFileSpecs);
        }

        private String configMapName(RemoteFileSpec rfs)
        {
            String name = rfs.fileSpec.getPath().replaceAll("[/.]", "-");
            if (!DNS1123.matcher(name).matches())
            {
                throw new InvalidConfigurationException(String.format(
                    "Remote kubernetes file paths must adhere to DNS-1123 format, '%s' is invalid",
                    rfs.fileSpec.getPath()));
            }
            return name;
        }

        @Override
        void validate(RemoteFileSpec rfs)
        {
            configMapName(rfs); // Ensure name is DNS-1123 compliant
        }

        @Override
        public String getConfigMapName(String path)
        {
            /*
            Users may reference a file which does not exist or files within a Git repo. In each case the
            reference path cannot be used to reconstruct the name used to deploy the files because the
            handler is starting with a different base path.
             */
            return configMapName(getReferencedSpec(path));
        }

        @Override
        public Path getFilePath(String path)
        {
            return Paths.get("/fallout-library/", getConfigMapName(path), path);
        }

        private KubeControlProvider.NamespacedKubeCtl kubeCtl(Optional<String> namespace)
        {
            return getNodeGroup()
                .findFirstRequiredProvider(KubeControlProvider.class)
                .createNamespacedKubeCtl(namespace);
        }

        @Override
        public boolean createRemoteFile(RemoteFileSpec rfs)
        {
            return kubeCtl(rfs.namespace).createConfigMap(configMapName(rfs), rfs.getLocalFilePath(getNodeGroup()));
        }

        @Override
        public boolean deleteRemoteFile(RemoteFileSpec rfs)
        {
            return kubeCtl(rfs.namespace)
                .execute(String.format("delete configmap %s", configMapName(rfs)))
                .waitForSuccess();
        }

        @Override
        public boolean remoteFileExists(RemoteFileSpec rfs)
        {
            return kubeCtl(rfs.namespace)
                .execute(String.format("get configmap %s", configMapName(rfs)))
                .waitForSuccess();
        }
    }
}
