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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeCommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.providers.NodeInfoProvider;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;

/**
 * Represents a machine/container
 *
 * Nodes can be created/configured/executed on/destroyed.
 *
 * Nodes can be in different States.
 * @see NodeGroup.State
 *
 */
public class Node implements HasProperties
{
    private static final Logger classLogger = LoggerFactory.getLogger(Node.class);

    private final NodeCommandExecutor nodeCommandExecutor;
    private final Provisioner provisioner;
    private final ConfigurationManager configurationManager;
    private final NodeGroup nodeGroup;
    private final int groupOrdinal;
    private final int ensembleOrdinal;
    private final Map<String, Provider> providers;
    final Logger logger;
    private final Path localArtifactPath;

    protected Node(NodeCommandExecutor nodeCommandExecutor, Provisioner provisioner,
        ConfigurationManager configurationManager, NodeGroup nodeGroup, int groupOrdinal,
        int ensembleOrdinal, JobLoggers loggers, Path localArtifactPath) throws IOException
    {
        this.nodeCommandExecutor = nodeCommandExecutor;
        this.provisioner = provisioner;
        this.configurationManager = configurationManager;
        this.nodeGroup = nodeGroup;
        this.groupOrdinal = groupOrdinal;
        this.ensembleOrdinal = ensembleOrdinal;
        this.localArtifactPath = localArtifactPath;
        this.providers = new ConcurrentHashMap<>();
        this.logger = loggers.create(getId(), Paths.get(nodeGroup.getName(), getFolderName(), "fallout-node.log"));

        Files.createDirectories(localArtifactPath);
    }

    public boolean waitForSuccess(String command)
    {
        return execute(command).waitForSuccess();
    }

    public boolean waitForSuccess(String command, Duration timeout)
    {
        return execute(command).waitForSuccess(timeout);
    }

    public boolean waitForQuietSuccess(String command)
    {
        return execute(command).waitForQuietSuccess();
    }

    public boolean waitForQuietSuccess(String command, Duration timeout)
    {
        return execute(command).waitForQuietSuccess(timeout);
    }

    /**
     * Executes a command on the node
     */
    public NodeResponse execute(String command)
    {
        return nodeCommandExecutor.execute(this, command);
    }

    /**
     * Executes a command on the node and captures the stdout/stderr.
     * Only do this if you absolutely need all of the output
     */
    public FullyBufferedNodeResponse executeBuffered(String command)
    {
        return execute(command).buffered();
    }

    /**
     *
     * @return the properties associated with this node
     */
    @Override
    public PropertyGroup getProperties()
    {
        return nodeGroup.getProperties();
    }

    /**
     *
     * @return the node group this node belongs to
     */
    public NodeGroup getNodeGroup()
    {
        return nodeGroup;
    }

    /**
     *
     * @return the nodes position in the associated nodegroup
     */
    public int getNodeGroupOrdinal()
    {
        return groupOrdinal;
    }

    /**
     *
     * @return the nodes position in the associated ensemble
     */
    public int getEnsembleOrdinal()
    {
        return ensembleOrdinal;
    }

    /**
    *
    * @return the provisioner used by this node
    */
    public Provisioner getProvisioner()
    {
        return provisioner;
    }

    /**
     *
     * @return the configuration manager used by this node
     */
    public ConfigurationManager getConfigurationManager()
    {
        return configurationManager;
    }

    /**
     * Associates a provider with a node
     *
     * @see Provider
     *
     * @param provider
     */
    protected void addProvider(Provider provider)
    {
        synchronized (providers)
        {
            if (providers.put(provider.name(), provider) != null)
                logger.info("Replacing existing provider " + provider.name());
            providers.notifyAll();
        }
    }

    /**
     * Removes a provider class from this node
     * @param provider
     */
    protected void removeProvider(Provider provider)
    {
        synchronized (providers)
        {
            providers.remove(provider.name());
        }
    }

    /**
     * Get the path on this node where its artifacts are/should be stored
     */
    public String getRemoteArtifactPath()
    {
        return provisioner.getRemoteArtifactPath(nodeGroup, Optional.of(this));
    }

    public String getRemoteScratchPath()
    {
        return provisioner.getRemoteScratchPath(nodeGroup, Optional.of(this));
    }

    public String getRemoteLibraryPath()
    {
        return provisioner.getRemoteLibraryPath(nodeGroup, Optional.of(this));
    }

    public boolean removeScratchSubDirectories(String... subDirs)
    {
        return removeSubDirectories(getRemoteScratchPath(), subDirs);
    }

    public boolean removeLibrarySubDirectories(String... subDirs)
    {
        return removeSubDirectories(getRemoteLibraryPath(), subDirs);
    }

    private boolean removeSubDirectories(String rootDir, String... subDirs)
    {
        if (subDirs.length == 0)
        {
            return true;
        }
        String rmCmd = "rm -rf";
        for (String subDir : subDirs)
        {
            rmCmd += String.format(" %s/%s", rootDir, subDir);
        }
        return this.waitForQuietSuccess(rmCmd);
    }

    /**
     * Transfers file or directory from remotePath at this node, to localPath. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param remotePath Path on remote node to transfer, can be file or dir
     * @param localPath Path to transfer to
     * @param deepCopy To recursively copy subdirs, if remotePath is a directory
     */
    public CompletableFuture<Boolean> get(String remotePath, Path localPath, boolean deepCopy)
    {
        return provisioner.get(this, remotePath, localPath, deepCopy);
    }

    /**
     * Transfers file or directory from localPath. to remotePath on this node. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param localPath Path to transfer from, can be file or dir
     * @param remotePath Path on remote node to transfer to
     * @param deepCopy To recursively copy subdirs, if localPath is a directory
     */
    public CompletableFuture<Boolean> put(Path localPath, String remotePath, boolean deepCopy)
    {
        return provisioner.put(this, localPath, remotePath, deepCopy);
    }

    /**
     * Transfers file or directory from localPath. to remotePath on this node. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param localPath Path to transfer from, can be file or dir
     * @param remotePath Path on remote node to transfer to
     * @param deepCopy To recursively copy subdirs, if localPath is a directory
     * @param permissions The file permissions to set on the remote node. Should be in octal
     */
    public CompletableFuture<Boolean> put(Path localPath, String remotePath, boolean deepCopy, int permissions)
    {
        return provisioner.put(this, localPath, remotePath, deepCopy, permissions);
    }

    /**
     * Transfers inputStream to remotePath on this node. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param inputStream Input Stream to transfer
     * @param remotePath Path on remote node to transfer to
     * @param permissions The file permissions to set on the remote node. Should be in octal
     */
    public CompletableFuture<Boolean> put(InputStream inputStream, String remotePath, int permissions)
    {
        return provisioner.put(this, inputStream, remotePath, permissions);
    }

    public CompletableFuture<Boolean> put(String content, String remotePath, int permissions)
    {
        var inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        return provisioner.put(this, inputStream, remotePath, permissions)
            .thenApplyAsync(result -> {
                Exceptions.runUncheckedIO(inputStream::close);
                return result;
            });
    }

    public <T extends Provider> boolean maybeUnregister(Class<T> clazz)
    {
        Optional<T> res = maybeGetProvider(clazz);
        res.ifPresent(Provider::unregister);
        return res.isPresent();
    }

    /**
     * Returns the Provider matching the specified provider class (or subclass)
     */
    public <T extends Provider> Optional<T> maybeGetProvider(Class<T> clazz)
    {
        synchronized (providers)
        {
            return providers.values().stream()
                .filter(clazz::isInstance)
                .map(clazz::cast).findFirst();
        }
    }

    public <T extends Provider> T getProvider(Class<T> clazz)
    {
        return maybeGetProvider(clazz)
            .orElseThrow(() -> new IllegalArgumentException("Node " + getId() + " has no provider: " + clazz));
    }

    public <T extends Provider> T waitForProvider(Class<T> clazz)
    {
        synchronized (providers)
        {
            Instant startTime = Instant.now();

            final java.time.Duration maxWait = java.time.Duration.ofMinutes(20);

            Optional<T> provider = maybeGetProvider(clazz);
            while (!provider.isPresent())
            {
                java.time.Duration elapsed = java.time.Duration.between(startTime, Instant.now());
                if (elapsed.compareTo(maxWait) > 0)
                {
                    final String message =
                        String.format("Timed out waiting for provider %s to be registered", clazz.getSimpleName());
                    logger.error(message);
                    throw new RuntimeException(message);
                }

                logger.info("Waiting for up to {}s for provider {} to be registered ({}s)...",
                    maxWait.getSeconds(), clazz.getSimpleName(), elapsed.getSeconds());
                Exceptions.runUninterruptibly(() -> providers.wait(60000));
                provider = maybeGetProvider(clazz);
            }
            return provider.get();
        }
    }

    public <T extends Provider> boolean hasProvider(Class<T> clazz)
    {
        return maybeGetProvider(clazz).isPresent();
    }

    public Collection<Provider> getAllProviders()
    {
        synchronized (providers)
        {
            return ImmutableList.copyOf(providers.values());
        }
    }

    public String getPublicAddress()
    {
        return getProvider(NodeInfoProvider.class).getPublicNetworkAddress();
    }

    public String waitForPublicAddress()
    {
        return waitForProvider(NodeInfoProvider.class).getPublicNetworkAddress();
    }

    public String getPrivateAddress()
    {
        return getProvider(NodeInfoProvider.class).getPrivateNetworkAddress();
    }

    public CompletableFuture<Boolean> prepareArtifacts()
    {
        return configurationManager.prepareArtifacts(this);
    }

    public CompletableFuture<Boolean> collectArtifacts()
    {
        return configurationManager.collectArtifacts(this);
    }

    public Logger logger()
    {
        return logger;
    }

    public String getId()
    {
        return nodeGroup.getId() + "-" + groupOrdinal;
    }

    public static String getFolderName(int groupOrdinal)
    {
        return "node" + groupOrdinal;
    }

    public String getFolderName()
    {
        return getFolderName(groupOrdinal);
    }

    public Path getLocalArtifactPath()
    {
        return localArtifactPath;
    }

    public enum ExistsCheckType
    {
        FILE("[ -f \"%s\" ]"),
        DIRECTORY("[ -d \"%s\" ]"),
        NON_EMPTY_DIRECTORY("[ \"$(ls -A '%s')\" ]");

        private final String checkCommand;

        ExistsCheckType(String command)
        {
            this.checkCommand = command;
        }
    }

    public boolean existsFile(String remotePathToFile)
    {
        return executeExistsCheck(ExistsCheckType.FILE, remotePathToFile);
    }

    public boolean existsDir(String remotePathToDir)
    {
        return executeExistsCheck(ExistsCheckType.DIRECTORY, remotePathToDir);
    }

    public boolean isNonEmptyDir(String remotePathToDir)
    {
        return executeExistsCheck(ExistsCheckType.NON_EMPTY_DIRECTORY, remotePathToDir);
    }

    public boolean executeExistsCheck(ExistsCheckType type, String pathToCheck)
    {
        boolean exists = execute(String.format(type.checkCommand, pathToCheck)).doWait()
            .withNonZeroIsNoError()
            .withDisabledTimeoutAfterNoOutput()
            .forSuccess();
        String locatedInfoStr = exists ? "" : "not ";
        logger.info("Check for {} at {} was {}successful", type, pathToCheck, locatedInfoStr);
        return exists;
    }

    @Override
    public String toString()
    {
        return "Node{" +
            ", provisioner=" + provisioner +
            ", configurationManager=" + configurationManager +
            ", nodeGroup=" + nodeGroup.getName() +
            ", nodeGroup.state=" + nodeGroup.getState() +
            ", groupOrdinal=" + groupOrdinal +
            ", providers=" + providers +
            '}';
    }
}
