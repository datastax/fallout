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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.harness.TestRunAbortedStatus;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory.LocalScratchSpace;
import com.datastax.fallout.ops.commands.NodeCommandExecutor;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.provisioner.NoRemoteAccessProvisioner;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.ops.NodeGroup.State.*;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.DOWN;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.NONE;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.UP;

/**
 * Represents a group of nodes.
 *
 * This could be a server group, client group, etc.
 */
public class NodeGroup implements HasProperties, DebugInfoProvidingComponent, AutoCloseable, HasAvailableProviders
{
    private static final Logger classLogger = LoggerFactory.getLogger(NodeGroup.class);

    final Logger logger;
    final Provisioner provisioner;
    final ConfigurationManager configurationManager;
    private final TestRunAbortedStatus testRunAbortedStatus;
    final WritablePropertyGroup properties;
    final ImmutableList<Node> nodes;
    final String name;
    final Set<String> aliases;
    final Ensemble.Role role;
    private final Path localArtifactPath;
    private final EnsembleCredentials credentials;
    private final LocalScratchSpace localScratchSpace;
    private final Optional<NodeGroup.State> finalRunLevel;
    private final HasAvailableProviders extraAvailableProviders;

    private volatile State state = UNKNOWN;
    private boolean hasStarted = false;

    protected NodeGroup(String name, Set<String> aliases, WritablePropertyGroup properties,
        NodeCommandExecutor nodeCommandExecutor, Provisioner provisioner,
        ConfigurationManager configurationManager,
        TestRunAbortedStatus testRunAbortedStatus, int nodeCount, Ensemble.Role role,
        IntSupplier ensembleOrdinalSupplier, JobLoggers loggers, Path localArtifactPath,
        EnsembleCredentials credentials, Optional<State> finalRunLevel,
        LocalScratchSpace localScratchSpace, HasAvailableProviders extraAvailableProviders)
    {
        this.name = name;
        this.aliases = aliases;
        this.properties = properties;
        this.provisioner = provisioner;
        this.configurationManager = configurationManager;
        this.testRunAbortedStatus = testRunAbortedStatus;
        this.role = role;
        this.localArtifactPath = localArtifactPath;
        this.credentials = credentials;
        this.finalRunLevel = finalRunLevel;
        this.logger = loggers.create(name, Paths.get(name, "fallout-nodegroup.log"));
        this.localScratchSpace = localScratchSpace;
        this.extraAvailableProviders = extraAvailableProviders;

        provisioner.setNodeGroup(this);
        configurationManager.setNodeGroup(this);
        configurationManager.setLogger(logger);

        FileUtils.createDirs(localArtifactPath);

        List<Node> nodeList = new ArrayList<>(nodeCount);
        for (int i = 0; i < nodeCount; i++)
        {
            nodeList.add(new Node(nodeCommandExecutor, provisioner, configurationManager, this, i,
                ensembleOrdinalSupplier.getAsInt(), loggers, localArtifactPath.resolve(Node.getFolderName(i))));
        }

        this.nodes = ImmutableList.copyOf(nodeList);
    }

    @Override
    public void close()
    {
        configurationManager.close();
    }

    public boolean currentOperationShouldBeAborted()
    {
        return testRunAbortedStatus.currentOperationShouldBeAborted();
    }

    @Override
    public PropertyGroup getProperties()
    {
        return properties;
    }

    public WritablePropertyGroup getWritableProperties()
    {
        return properties;
    }

    public String getName()
    {
        return name;
    }

    public ImmutableList<Node> getNodes()
    {
        return nodes;
    }

    public Logger logger()
    {
        return logger;
    }

    public String getId()
    {
        if (role == null || role.name().equalsIgnoreCase(name))
        {
            return name;
        }
        // multi cluster can have multiple nodegroups with the same role
        return role + "-" + name;
    }

    public Provisioner getProvisioner()
    {
        return provisioner;
    }

    public ConfigurationManager getConfigurationManager()
    {
        return configurationManager;
    }

    public Optional<String> explicitClusterName()
    {
        return provisioner.explicitClusterName(this);
    }

    public String clusterName()
    {
        return provisioner.clusterName(this);
    }

    public Ensemble.Role getRole()
    {
        return role;
    }

    /**
     *  Get the path on each node in this nodeGroup where their artifacts are/should be stored
     * @return
     */
    public String getRemoteArtifactPath()
    {
        return provisioner.getRemoteArtifactPath(this, Optional.empty());
    }

    /**
     * Get the path on each node in this nodeGroup where their scratch directory
     * for holding scripts
     * @return
     */
    public String getRemoteScratchPath()
    {
        return provisioner.getRemoteScratchPath(this, Optional.empty());
    }

    public String getRemoteLibraryPath()
    {
        return provisioner.getRemoteLibraryPath(this, Optional.empty());
    }

    public Path getLocalArtifactPath()
    {
        return localArtifactPath;
    }

    public Optional<ResourceRequirement> getResourceRequirements()
    {
        return provisioner.getResourceRequirements(this);
    }

    /**
     *
     * @return the current state of the node group
     */
    public State getState()
    {
        return state;
    }

    public boolean isMarkedForReuse()
    {
        return getFinalRunLevel().filter(finalRunLevel_ -> finalRunLevel_ == DESTROYED).isEmpty();
    }

    /** The state that the {@link NodeGroup} should be left in after tearing down at the end of a test run; if
     *  empty, then the {@link NodeGroup} should be left in whatever state it was in at the end of the workload.
     *
     *  <p>This is set by the mutually exclusive properties:
     *
     *  <ul>
     *      <li><code>mark_for_reuse</code>: if <code>true</code> then this will be empty;
     *      <li><code>runlevel.final</code>: if set, then this will be the set value;
     *      <li>otherwise, it defaults to {@link State#DESTROYED}.
     * </ul>
     */
    public Optional<State> getFinalRunLevel()
    {
        return finalRunLevel;
    }

    public LocalScratchSpace getLocalScratchSpace()
    {
        return localScratchSpace;
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders()
    {
        return Stream.of(provisioner, configurationManager, extraAvailableProviders)
            .map(HasAvailableProviders::getAvailableProviders)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());
    }

    public boolean createLocalFile(Path fileLocation, FileSpec fileSpec)
    {
        return fileSpec.createLocalFile(this.logger(), this.getProvisioner().getCommandExecutor(), fileLocation);
    }

    public boolean willHaveProvider(Class<? extends Provider> reqProvider)
    {
        return getAvailableProviders().stream().anyMatch(reqProvider::isAssignableFrom);
    }

    /**
     * Changes the node group state.  Not to be called directly see @NodeStateManager
     * @param newState the new state
     */
    void setState(State newState)
    {
        String logMsg = "Setting node group {} state {} -> {}";
        boolean toUnknown = this.state != UNKNOWN && newState == UNKNOWN;
        boolean toFailed = newState == FAILED;
        if (toUnknown)
        {
            logger.warn(logMsg, getId(), this.state, newState);
        }
        else if (toFailed)
        {
            logger.error(logMsg, getId(), this.state, newState);
        }
        else
        {
            logger.info(logMsg, getId(), this.state, newState);
        }
        if (newState.isStarted())
        {
            hasStarted = true;
        }
        this.state = newState;
    }

    /**
     * Inspects the node group when its current state is unknown and sets the detected state.
     * Returns a BooleanSupplier that should be eventually run the first time its called.
     */
    public CompletableFuture<PostCheckStateActionsForAsyncExecution> checkState()
    {
        if (!state.isUnknownState())
        {
            return CompletableFuture.completedFuture(() -> true);
        }

        return provisioner.checkState(this)
            // get state as seen by provisioner or configuration manager
            .thenComposeAsync(checkedState -> {
                logger.info("Provisioner.checkState() returned {}", checkedState);

                if (checkedState.isConfigManagementState())
                {
                    if (checkedState.compareTo(STARTED_SERVICES_UNCONFIGURED) >= 0)
                    {
                        logger.info("Provisioner.checkState() was greater than {}; cleaning directories...",
                            STARTED_SERVICES_UNCONFIGURED);
                        provisioner.cleanDirectoriesBeforeTestRun(this);
                    }

                    logger.info("Provisioner.checkState() returned ConfigurationManager state {}; " +
                        "checking with ConfigurationManager...", checkedState);
                    return configurationManager.checkState(this)
                        .thenApplyAsync(configurationManagerState -> {
                            logger.info("ConfigurationManager.checkState() returned {}", configurationManagerState);

                            if (configurationManagerState.isUnknownState())
                            {
                                logger.info("ConfigurationManager.checkState() returned unknown state {} " +
                                    "using previous value of {} from Provisioner.checkState()",
                                    configurationManagerState, checkedState);
                                return checkedState;
                            }
                            return configurationManagerState;
                        });
                }

                return CompletableFuture.completedFuture(checkedState);
            })
            // handle updating NodeGroup state
            .thenComposeAsync(checkedState -> {
                if (checkedState.isTransitioningState())
                {
                    logger.error("NodeGroup.checkState() got a transitioning state {}; this is not allowed, " +
                        "and is being treated as FAILED", checkedState);
                    checkedState = FAILED;
                }

                if (checkedState.isUnknownState())
                {
                    // If the current state is UNKNOWN, then we're definitely at
                    // DESTROYED.  Otherwise, if it's FAILED, then we use RESERVED
                    // as that will ensure we still attempt to transition out of that state.
                    State fallback = getState() == UNKNOWN ? DESTROYED : RESERVED;

                    logger.warn("NodeGroup.checkState() got an unknown state {}; using {} instead",
                        checkedState, fallback);

                    checkedState = fallback;
                }

                if (getState() == FAILED && checkedState.compareTo(STOPPED) < 0)
                {
                    logger.warn("Current node group state is FAILED, but NodeGroup.checkState() got {}, " +
                        "which is < STOPPED; using STOPPED", checkedState);
                    checkedState = STOPPED;
                }

                PostCheckStateActionsForAsyncExecution postCheckStateActions = () -> true;

                // Potentially handle cluster reuse the first time we check state.
                if (getState() == UNKNOWN)
                {
                    if (checkedState.compareTo(STARTED_SERVICES_CONFIGURED) >= 0)
                    {
                        postCheckStateActions = postCheckStateActions
                            .andThen(() -> nodes.stream().allMatch(configurationManager::registerProviders));
                    }
                }

                setState(checkedState);

                return CompletableFuture.completedFuture(postCheckStateActions);
            });
    }

    public interface PostCheckStateActionsForAsyncExecution extends BooleanSupplier
    {
        default PostCheckStateActionsForAsyncExecution andThen(PostCheckStateActionsForAsyncExecution next)
        {
            return () -> this.getAsBoolean() && next.getAsBoolean();
        }
    }

    //
    // Transition Methods
    //

    private <T> CompletableFuture<T> progressivelyApplyAction(
        BiFunction<State, State, Optional<Pair<Function<NodeGroup, T>, State.ActionStates>>> actionSupplier,
        T success, T failure, Function<T, Boolean> wasSuccessful,
        final State endState)
    {
        // Execute the next node group action; after it completes schedule this
        // method to be called again.  If there's no action, then just return.

        if (testRunAbortedStatus.hasBeenAborted() && state.direction(endState) != DOWN)
        {
            return CompletableFuture.completedFuture(success);
        }

        State.checkValidTransition(state, endState);

        return actionSupplier.apply(state, endState)
            .map(actionAndStates ->
            // perform NodeGroup action
            handleAction(actionAndStates.getLeft(), actionAndStates.getRight(), success, failure)
                .thenComposeAsync(actionResult -> wasSuccessful.apply(actionResult) ?
                    // previous action was successful, continue transition to endState.
                    progressivelyApplyAction(actionSupplier, success, failure, wasSuccessful, endState) :
                    // previous action failed, stop transition.
                    CompletableFuture.completedFuture(actionResult)
                ))
            // There is no next NodeGroup action. We are as close to endState as this method will allow.
            .orElse(CompletableFuture.completedFuture(success));
    }

    /** Returns a CompletableFuture that, when complete, indicates that the
     *  node group has completed all transitions that involve a group resource action. */
    private CompletableFuture<CheckResourcesResult> progressivelyApplyResourceAction(final State endState)
    {
        return progressivelyApplyAction(State::nextResourceAction,
            CheckResourcesResult.AVAILABLE, CheckResourcesResult.FAILED, CheckResourcesResult::wasSuccessful,
            endState);
    }

    /** Returns a CompletableFuture that, when complete, indicates that the
     *  node group has completed all transitions that involve a group boolean action. */
    private CompletableFuture<Boolean> progressivelyApplyBooleanAction(final State endState)
    {
        return progressivelyApplyAction(State::nextBooleanAction,
            true, false, Function.identity(),
            endState);
    }

    private CompletableFuture<Void> maybeCheckState(final State endState)
    {
        return CompletableFuture
            .runAsync(() -> {
                Preconditions.checkArgument(endState.isRunLevelState(),
                    "Can't transition node group to invalid state %s", endState);
            })
            .thenComposeAsync(ignored -> checkState().thenApplyAsync(ignored_ -> null));
    }

    public CompletableFuture<CheckResourcesResult> transitionStateIfDirectionIs(final State endState,
        State.TransitionDirection direction)
    {
        return maybeCheckState(endState)
            .thenComposeAsync(ignored -> {
                if (state.direction(endState) != direction)
                {
                    logger.info("Not transitioning {} {} from ({}) -> {}",
                        name, direction, state, endState);
                    return CompletableFuture.completedFuture(CheckResourcesResult.AVAILABLE);
                }

                return transitionState(endState);
            })
            .exceptionally(ex -> {
                logger.error("Error transitioning nodes:", ex);
                return CheckResourcesResult.FAILED;
            });
    }

    public CompletableFuture<CheckResourcesResult> transitionStateIfUpwards(final State endState)
    {
        return transitionStateIfDirectionIs(endState, UP);
    }

    public CompletableFuture<CheckResourcesResult> transitionStateIfDownwards(final State endState)
    {
        return transitionStateIfDirectionIs(endState, DOWN);
    }

    /**
     * Transitions the NodeGroup to endState, regardless of direction. Will go through all states in between
     * current state and endState.
     * @param endState
     * @return
     */
    public CompletableFuture<CheckResourcesResult> transitionState(final State endState)
    {
        final var logMessage = String.format("Transitioning node group %s from %s -> %s...", name, state, endState);

        return
        // maybeCheckState will ensure NodeGroup.state is correct
        maybeCheckState(endState)
            // Do group transitions below endState
            .thenComposeAsync(ignored -> {
                logger.info(logMessage);
                return progressivelyApplyResourceAction(endState);
            })
            .thenComposeAsync(resourceTransitions -> {
                if (!resourceTransitions.wasSuccessful())
                {
                    return CompletableFuture.completedFuture(resourceTransitions);
                }

                return progressivelyApplyBooleanAction(endState)
                    .thenApplyAsync(CheckResourcesResult::fromWasSuccessful);
            })
            .exceptionally(ex -> {
                logger.error(logMessage + "failed", ex);
                return CheckResourcesResult.FAILED;
            })
            .thenApplyAsync(transitionResult -> {
                switch (transitionResult)
                {
                    case UNAVAILABLE:
                        logger.warn("{}failed (resources unavailable)", logMessage);
                        break;
                    case FAILED:
                        logger.error("{}failed", logMessage);
                        break;
                    default:
                        logger.info("{}done", logMessage);
                        break;
                }
                return transitionResult;
            });
    }

    //
    // Action Methods
    //
    private <T> CompletableFuture<T> handleAction(Function<NodeGroup, T> action,
        State.ActionStates actionStates, T success, T failure)
    {
        return CompletableFuture.supplyAsync(() -> {
            setState(actionStates.transition);

            T result = action.apply(this);

            if (result != success)
            {
                setState(FAILED);
                return result;
            }

            setState(actionStates.runLevel);

            return result;
        })
            .exceptionally(e -> {
                logger.error("Error encountered during {}: {}", actionStates.transition, e);
                setState(FAILED);
                return failure;
            });
    }

    synchronized private CheckResourcesResult reserve()
    {
        return provisioner.reserveImpl(this);
    }

    synchronized private CheckResourcesResult create()
    {
        return provisioner.createImpl(this);
    }

    synchronized private boolean prepare()
    {
        return provisioner.prepareImpl(this);
    }

    synchronized private boolean start()
    {
        logger.info("Starting nodegroup...");
        boolean startSuccess = provisioner.startImpl(this);
        if (!startSuccess)
        {
            logger.error("Nodegroup start failed!");
            return false;
        }

        logger.info("Cleaning scratch and artifact directories...");
        boolean cleanDirectoriesSuccess = provisioner.cleanDirectoriesBeforeTestRun(this);
        if (!cleanDirectoriesSuccess)
        {
            logger.error("Cleaning scratch and artifact directories failed!");
            return false;
        }
        return true;
    }

    synchronized private boolean configure()
    {
        return configurationManager.configureAndRegisterProviders(this);
    }

    synchronized private boolean startServices()
    {
        return configurationManager.startImpl(this);
    }

    synchronized private boolean stopServices()
    {
        return configurationManager.stopImpl(this);
    }

    synchronized private boolean unconfigure()
    {
        return configurationManager.unconfigureImpl(this) &&
            this.waitForAllNodes(configurationManager::unregisterProviders, "unregister providers");
    }

    synchronized private boolean stop()
    {
        return provisioner.stopImpl(this);
    }

    synchronized private boolean destroy()
    {
        return provisioner.destroyImpl(this);
    }

    public void summarizeInfo(InfoConsumer infoConsumer)
    {
        HashMap<String, Object> info = new HashMap<>();
        getProvisioner().summarizeInfo(info::put);
        getConfigurationManager().summarizeInfo(info::put);
        infoConsumer.accept(getName(), info);
    }

    //
    // Utility Methods
    //

    /**
     * ForceDestroys this nodeGroup
     * @return
     */
    synchronized public CompletableFuture<Boolean> forceDestroy()
    {
        return CompletableFuture.supplyAsync(() -> {
            if (state == DESTROYED)
            {
                logger.warn("NodeGroup in destroyed state. Are you sure you have to forceDestroy()?");
                return true;
            }
            return provisioner.destroyImpl(this);
        })
            .exceptionally(e -> {
                logger.error("Error encountered during forceDestroy() ", e);
                return false;
            });
    }

    private <T extends Provider> Supplier<IllegalArgumentException> missingProviderError(Class<T> providerClass)
    {
        return () -> new IllegalArgumentException(
            "NodeGroup '" + getId() + "' is missing a Node with a " + providerClass.getSimpleName());
    }

    public <T extends Provider> List<T> findAllProviders(Class<T> providerClass, Predicate<T> predicate)
    {
        return getNodes().stream()
            .flatMap(node -> node.getAllProviders().stream())
            .filter(providerClass::isInstance)
            .map(providerClass::cast)
            .filter(predicate)
            .collect(Collectors.toList());
    }

    public <T extends Provider> Optional<T> findFirstProvider(Class<T> providerClass)
    {
        return findFirstNodeWithProvider(providerClass).map(n -> n.getProvider(providerClass));
    }

    public <T extends Provider> T findFirstRequiredProvider(Class<T> providerClass)
    {
        return findFirstProvider(providerClass).orElseThrow(missingProviderError(providerClass));
    }

    public Optional<Node> findFirstNodeWithProvider(Class<? extends Provider> providerClass)
    {
        return getNodes().stream().filter(n -> n.hasProvider(providerClass)).findFirst();
    }

    public Node findFirstNodeWithRequiredProvider(Class<? extends Provider> providerClass)
    {
        return findFirstNodeWithProvider(providerClass).orElseThrow(missingProviderError(providerClass));
    }

    public void maybeUnregisterProviders(Class<? extends Provider> providerClass)
    {
        nodes.forEach(n -> n.maybeUnregister(providerClass));
    }

    public boolean allNodesHaveProvider(Class<? extends Provider> providerClass)
    {
        return nodes.stream().allMatch(n -> n.hasProvider(providerClass));
    }

    public CompletableFuture<Boolean> prepareArtifacts()
    {
        if (state != STARTED_SERVICES_RUNNING)
        {
            logger.info(
                "NodeGroup.prepareArtifacts will do nothing since the node group is not STARTED_SERVICES_RUNNING");
            return CompletableFuture.completedFuture(true);
        }

        return waitForAsyncNodeFunctionAsync(Node::prepareArtifacts, "collecting artifacts from nodegroup");
    }

    public CompletableFuture<Boolean> collectArtifacts()
    {
        if (!hasStarted)
        {
            logger.info("NodeGroup.collectArtifacts will do nothing since the NodeGroup was never started.");
            return CompletableFuture.completedFuture(true);
        }

        return waitForAsyncNodeFunctionAsync(Node::collectArtifacts, "collecting artifacts from nodegroup");
    }

    public CompletableFuture<Boolean> downloadArtifacts()
    {
        if (!hasStarted)
        {
            logger.info("NodeGroup.downloadArtifacts will do nothing since the NodeGroup was never started.");
            return CompletableFuture.completedFuture(true);
        }

        CompletableFuture<Boolean> generalNodeArtifacts;

        if (provisioner instanceof NoRemoteAccessProvisioner)
        {
            logger.info(
                "NodeGroup.downloadArtifacts will not download the node's artifact directories since SSH is not available on NoRemoteAccessProvisioner");
            generalNodeArtifacts = CompletableFuture.completedFuture(true);
        }
        else
        {
            generalNodeArtifacts = waitForAllNodesAsync(
                node -> node.get(node.getRemoteArtifactPath(), node.getLocalArtifactPath(), true).join(),
                "getting general node artifacts");
        }

        CompletableFuture<Boolean> nodeProvisionerArtifacts = waitForAllNodesAsync(
            node -> getProvisioner().downloadProvisionerArtifacts(node, node.getLocalArtifactPath()).join(),
            "getting node provisioner artifacts");

        CompletableFuture<Boolean> nodeGroupProvisionerArtifacts =
            getProvisioner().downloadProvisionerArtifacts(this, getLocalArtifactPath());

        return Utils.waitForAllAsync(
            List.of(generalNodeArtifacts, nodeProvisionerArtifacts, nodeGroupProvisionerArtifacts));
    }

    /**
     * Will download file from specified URL to specified local path
     * @param url: The URL where we download file from
     * @param pathOnNode: The path to download file to
     */
    public boolean download(String url, String pathOnNode)
    {
        String command = String.format("wget '%s' -O '%s'", url, pathOnNode);
        return waitForSuccess(command);
    }

    /**
     * Transfers file or directory from remotePath for each node in this nodeGroup, to localPath/nodeX where X is each node's ordinal.
     * In case of a directory, will transfer all files in the directory, and subdirs iff deepCopy is true
     * @param remotePath Path on remote node to transfer, can be file or dir
     * @param localPath Path to transfer to
     * @param deepCopy To recursively copy subdirs, if remotePath is a directory
     * @return
     */
    public CompletableFuture<Boolean> get(String remotePath, Path localPath, boolean deepCopy)
    {
        return waitForAsyncNodeFunctionAsync(
            node -> node.get(remotePath, localPath.resolve(node.getFolderName()), deepCopy),
            "getting from nodes");
    }

    /**
     * Transfers file or directory from localPath to remotePath on each node in this nodeGroup. In case of a directory,
     * will transfer all files in the directory, and subdirs iff deepCopy is true
     * @param localPath Path to transfer from, can be file or dir
     * @param remotePath Path on remote node to transfer to
     * @param deepCopy To recursively copy subdirs, if localPath is a directory
     * @return
     */
    public CompletableFuture<Boolean> put(Path localPath, String remotePath, boolean deepCopy)
    {
        return waitForAsyncNodeFunctionAsync(node -> node.put(localPath, remotePath, deepCopy), "putting to nodes");
    }

    /**
     * Transfers inputStream to remotePath on each node in this nodeGroup. In case of a directory, will transfer all
     * files in the directory, and subdirs iff deepCopy is true
     * @param inputStream Input Stream to transfer
     * @param remotePath Path on remote node to transfer to
     * @param permissions The file permissions to set on the remote node. Should be in octal
     */
    public CompletableFuture<Boolean> put(InputStream inputStream, String remotePath, int permissions)
    {
        return waitForAsyncNodeFunctionAsync(node -> node.put(inputStream, remotePath, permissions),
            "putting to nodes");
    }

    /**
     * Transfers file or directory from localPath. to remotePath on each node in this nodeGroup. In case of a directory,
     * will transfer all files in the directory, and subdirs iff deepCopy is true
     * @param localPath Path to transfer from, can be file or dir
     * @param remotePath Path on remote node to transfer to
     * @param deepCopy To recursively copy subdirs, if localPath is a directory
     * @param permissions The file permissions to set on the remote node
     * @return
     */
    public CompletableFuture<Boolean> put(Path localPath, String remotePath, boolean deepCopy, int permissions)
    {
        return waitForAsyncNodeFunctionAsync(node -> node.put(localPath, remotePath, deepCopy, permissions),
            "putting to nodes");
    }

    /**
     * Transfers a content to remotePath on each node in this nodeGroup.
     *
     * @param content Content to write in the remote file
     * @param remotePath Path on remote node to transfer to
     * @param permissions The file permissions to set on the remote node
     * @return a {@link CompletableFuture} that will succeed when all the nodes have received the file
     */
    public CompletableFuture<Boolean> put(String content, String remotePath, int permissions)
    {
        return waitForAsyncNodeFunctionAsync(node -> {
            InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
            return node.put(inputStream, remotePath, permissions);
        }, "putting to node");
    }

    public boolean waitForSuccess(String script)
    {
        return waitForNodeSpecificSuccess(n -> script);
    }

    public boolean waitForSuccess(String script, Duration timeout)
    {
        return waitForNodeSpecificSuccess(n -> script, wo -> wo.timeout = timeout);
    }

    public boolean waitForSuccess(String script, NodeResponse.WaitOptionsAdjuster waitOptionsAdjuster)
    {
        return waitForNodeSpecificSuccess(n -> script, waitOptionsAdjuster);
    }

    public boolean waitForSuccessWithNonZeroIsNoError(String script)
    {
        return waitForSuccess(script, wo -> wo.nonZeroIsNoError());
    }

    public boolean waitForNodeSpecificSuccess(Function<Node, String> nodeSpecificScriptCreator)
    {
        return waitForNodeSpecificSuccess(nodeSpecificScriptCreator, wo -> {});
    }

    public boolean waitForNodeSpecificSuccess(Function<Node, String> nodeSpecificScriptCreator, Duration timeout)
    {
        return waitForNodeSpecificSuccess(nodeSpecificScriptCreator, wo -> wo.timeout = timeout);
    }

    public boolean waitForNodeSpecificSuccess(Function<Node, String> nodeSpecificScriptCreator,
        NodeResponse.WaitOptionsAdjuster waitOptionsAdjuster)
    {
        List<NodeResponse> futures = nodes.stream()
            .map(n -> n.execute(nodeSpecificScriptCreator.apply(n)))
            .collect(Collectors.toList());
        return Utils.waitForSuccess(logger, futures, waitOptionsAdjuster);
    }

    public CompletableFuture<Boolean> waitForAsyncNodeFunctionAsync(Function<Node, CompletableFuture<Boolean>> func,
        String operation)
    {
        List<CompletableFuture<Boolean>> futures = nodes.stream()
            .map(func::apply)
            .collect(Collectors.toList());

        return Utils.waitForAllAsync(futures, logger, operation);
    }

    public boolean waitForAllNodes(Function<Node, Boolean> func, String operation)
    {
        return waitForAllNodesAsync(func, operation).join();
    }

    public CompletableFuture<Boolean> waitForAllNodesAsync(Function<Node, Boolean> func, String operation)
    {
        List<CompletableFuture<Boolean>> futures = nodes.stream()
            .map(node -> CompletableFuture.supplyAsync(() -> func.apply(node)))
            .collect(Collectors.toList());

        return Utils.waitForAllAsync(futures, logger, operation);
    }

    public boolean removeNodeSpecificScratchDirectories(String... subDirs)
    {
        return removeNodeSpecificDirectories(node -> node.removeScratchSubDirectories(subDirs),
            String.format("removing %s scratch directories", (Object[]) subDirs));
    }

    public boolean removeNodeSpecificLibraryDirectories(String... subDirs)
    {
        return removeNodeSpecificDirectories(node -> node.removeLibrarySubDirectories(subDirs),
            String.format("removing %s library directories", (Object[]) subDirs));
    }

    private boolean removeNodeSpecificDirectories(Function<Node, Boolean> removalFunc, String operation)
    {
        return waitForAllNodes(removalFunc, operation);
    }

    @Override
    public String toString()
    {
        return "NodeGroup{" +
            "provisioner=" + provisioner +
            ", configurationManager=" + configurationManager +
            ", properties=" + properties +
            ", nodes=" + nodes +
            ", name='" + name + "'" +
            ", role=" + role +
            ", state=" + state +
            ", aliases='" + String.join(", ", aliases) + "'" +
            '}';
    }

    /**
     * Represents the lifecycle states of a node.
     * There are valid state transitions through which a node can proceed.
     *
     * This enum class can validate those transitions.
     *
     * There is some ordering to this enum.
     * The ordering ties with the temporal ordering of a cluster lifecycle.
     * From setting up of cluster to tearing down. Like a linux runlevel.
     *
     */
    public enum State
    {
        // Unknown States

        /** No idea what's going on */
        UNKNOWN(true),
        /** The node is failed (almost like unknown, but at least know something bad happened) */
        FAILED(true),

        // We use these to transition between states

        /** In the process of reserving/provisioning the group */
        RESERVING,
        CREATING,
        /** The group is in the process of being prepared */
        PREPARING,
        /** The group is in the process of being destroyed */
        DESTROYING,

        /** The node is in the process of starting up */
        STARTING,
        /** The node is stopping */
        STOPPING,

        /** The node is in the process of being configured */
        STARTED_CONFIGURING_SERVICES,
        /** The node is in the process of being unconfigured */
        STARTED_UNCONFIGURING_SERVICES,

        /** The nodes services are starting */
        STARTED_STARTING_SERVICES,
        /** The nodes services are stopping */
        STARTED_STOPPING_SERVICES,

        // RunLevel States. These are stable states a node/cluster can be in (non-transitional)

        /** The group was destroyed OR does not exist yet */
        DESTROYED(RESERVING, null),
        /** The node is created/reserved */
        RESERVED(CREATING, DESTROYING),
        CREATED(PREPARING, DESTROYING),
        /** The node is stopped, but still reserved (power off) */
        STOPPED(STARTING, DESTROYING),
        /** The node is started */
        STARTED_SERVICES_UNCONFIGURED(STARTED_CONFIGURING_SERVICES, STOPPING),
        /** The node is started and configured. Services are stopped */
        STARTED_SERVICES_CONFIGURED(STARTED_STARTING_SERVICES, STARTED_UNCONFIGURING_SERVICES),
        /** The node is started configures and services are running (final best state) */
        STARTED_SERVICES_RUNNING(null, STARTED_STOPPING_SERVICES);

        static
        {
            RESERVING.transition = Transition.createResource(RESERVED, NodeGroup::reserve);
            CREATING.transition = Transition.createResource(CREATED, NodeGroup::create);
            PREPARING.transition = Transition.createBoolean(STOPPED, NodeGroup::prepare);
            DESTROYING.transition = Transition.createBoolean(DESTROYED, NodeGroup::destroy);

            STARTING.transition = Transition.createBoolean(STARTED_SERVICES_UNCONFIGURED, NodeGroup::start);
            STOPPING.transition = Transition.createBoolean(STOPPED, NodeGroup::stop);

            STARTED_CONFIGURING_SERVICES.transition =
                Transition.createBoolean(STARTED_SERVICES_CONFIGURED, NodeGroup::configure);
            STARTED_UNCONFIGURING_SERVICES.transition =
                Transition.createBoolean(STARTED_SERVICES_UNCONFIGURED, NodeGroup::unconfigure);

            STARTED_STARTING_SERVICES.transition =
                Transition.createBoolean(STARTED_SERVICES_RUNNING, NodeGroup::startServices);
            STARTED_STOPPING_SERVICES.transition =
                Transition.createBoolean(STARTED_SERVICES_CONFIGURED, NodeGroup::stopServices);
        }

        //Tracks the allowed state transitions.
        // All transitions *to* FAILED are allowed
        // All transitions *from* unknown states are allowed
        private final static Multimap<State, State> legalTransitions = ArrayListMultimap.create();
        static
        {
            legalTransitions.put(DESTROYED, RESERVING);

            legalTransitions.put(RESERVING, RESERVED);

            legalTransitions.put(RESERVED, DESTROYING);
            legalTransitions.put(RESERVED, CREATING);

            legalTransitions.put(CREATING, CREATED);

            legalTransitions.put(CREATED, DESTROYING);
            legalTransitions.put(CREATED, PREPARING);

            legalTransitions.put(PREPARING, STOPPED);

            legalTransitions.put(STARTING, STARTED_SERVICES_UNCONFIGURED);
            legalTransitions.put(STARTING, STOPPING);
            legalTransitions.put(STARTING, DESTROYING);

            legalTransitions.put(STARTED_SERVICES_UNCONFIGURED, STARTED_CONFIGURING_SERVICES);
            legalTransitions.put(STARTED_CONFIGURING_SERVICES, STARTED_SERVICES_CONFIGURED);

            legalTransitions.put(STARTED_SERVICES_CONFIGURED, STARTED_STARTING_SERVICES);
            legalTransitions.put(STARTED_SERVICES_RUNNING, STARTED_SERVICES_CONFIGURED);
            legalTransitions.put(STARTED_SERVICES_RUNNING, STOPPING);
            legalTransitions.put(STARTED_SERVICES_RUNNING, DESTROYING);
            legalTransitions.put(STARTED_SERVICES_RUNNING, STARTED_STOPPING_SERVICES);
            legalTransitions.put(STARTED_STARTING_SERVICES, STARTED_SERVICES_RUNNING);
            legalTransitions.put(STARTED_STOPPING_SERVICES, STARTED_SERVICES_CONFIGURED);

            legalTransitions.put(STARTED_SERVICES_CONFIGURED, STARTED_UNCONFIGURING_SERVICES);
            legalTransitions.put(STARTED_UNCONFIGURING_SERVICES, STOPPING);
            legalTransitions.put(STARTED_UNCONFIGURING_SERVICES, DESTROYING);
            legalTransitions.put(STARTED_UNCONFIGURING_SERVICES, STARTED_SERVICES_UNCONFIGURED);

            legalTransitions.put(STARTED_SERVICES_UNCONFIGURED, STOPPING);
            legalTransitions.put(STARTED_SERVICES_UNCONFIGURED, DESTROYING);

            legalTransitions.put(STARTED_SERVICES_CONFIGURED, STOPPING);
            legalTransitions.put(STARTED_SERVICES_CONFIGURED, DESTROYING);

            legalTransitions.put(STOPPING, STOPPED);
            legalTransitions.put(STOPPING, DESTROYING);

            legalTransitions.put(DESTROYING, DESTROYED);
            legalTransitions.put(STOPPED, STARTING);
            legalTransitions.put(STOPPED, DESTROYING);
        }

        public static class ActionStates
        {
            final State transition;
            final State runLevel;

            public ActionStates(State transition, State runLevel)
            {
                this.transition = transition;
                this.runLevel = runLevel;
            }
        }

        static class Transition
        {
            final State next;
            private final Optional<Function<NodeGroup, CheckResourcesResult>> resourceAction;
            private final Optional<Function<NodeGroup, Boolean>> booleanAction;

            private Transition(
                State next,
                Optional<Function<NodeGroup, CheckResourcesResult>> resourceAction,
                Optional<Function<NodeGroup, Boolean>> booleanAction)
            {
                Preconditions.checkNotNull(next);
                Preconditions.checkArgument(resourceAction.isPresent() || booleanAction.isPresent());

                this.next = next;
                this.resourceAction = resourceAction;
                this.booleanAction = booleanAction;
            }

            private static Transition createResource(
                State nextState,
                Function<NodeGroup, CheckResourcesResult> action)
            {
                return new Transition(nextState, Optional.of(action), Optional.empty());
            }

            private static Transition createBoolean(
                State nextState,
                Function<NodeGroup, Boolean> action)
            {
                return new Transition(nextState, Optional.empty(), Optional.of(action));
            }

            TransitionDirection direction(State endState)
            {
                return next.direction(endState);
            }
        }

        static class RunLevel
        {
            final State up;
            final State down;

            private RunLevel(State up, State down)
            {
                this.up = up;
                this.down = down;

                Preconditions.checkArgument(this.up != null || this.down != null);
            }

            Optional<State> up()
            {
                return Optional.ofNullable(up);
            }

            Optional<State> down()
            {
                return Optional.ofNullable(down);
            }

            Optional<Transition> upTransition()
            {
                return up().flatMap(s -> Optional.ofNullable(s.transition));
            }

            Optional<Transition> downTransition()
            {
                return down().flatMap(s -> Optional.ofNullable(s.transition));
            }
        }

        final RunLevel runLevel;
        private Transition transition = null;
        private final boolean isUnknown;

        static
        {
            Arrays.stream(State.values()).forEach(state -> {
                if (state.runLevel != null)
                {
                    Preconditions.checkState(state.transition == null);
                    Preconditions.checkState(!state.isUnknown);
                    Preconditions.checkState(
                        state.runLevel.upTransition().isPresent() || state.runLevel.downTransition().isPresent());
                }

                if (state.transition != null)
                {
                    Preconditions.checkState(state.runLevel == null);
                    Preconditions.checkState(!state.isUnknown);
                    Preconditions.checkState(state.transition.next.runLevel != null);
                }
            });
        }

        State()
        {
            this.isUnknown = false;
            this.runLevel = null;
        }

        State(boolean isUnknown)
        {
            this.isUnknown = isUnknown;
            this.runLevel = null;
        }

        State(State up, State down)
        {
            this.isUnknown = false;
            this.runLevel = new RunLevel(up, down);
        }

        public boolean isTransitioningState()
        {
            return transition != null;
        }

        public boolean isConfigManagementState()
        {
            switch (this)
            {
                case STARTED_SERVICES_RUNNING:
                case STARTED_SERVICES_UNCONFIGURED:
                case STARTED_SERVICES_CONFIGURED:
                    return true;
                default:
                    return false;
            }
        }

        public boolean isUnknownState()
        {
            return isUnknown;
        }

        public boolean isRunLevelState()
        {
            return runLevel != null;
        }

        public boolean valid(NodeGroup nodeGroup, State next)
        {
            boolean valid =
                this == next || next == FAILED || legalTransitions.containsEntry(this, next) || this.isUnknownState();

            if (!valid)
                nodeGroup.logger.warn("Invalid " + this + " => " + next);

            return valid;
        }

        public static State checkValidTransition(State startState, State endState)
        {
            Preconditions.checkState((startState.isRunLevelState() || startState.isUnknownState()) &&
                endState.isRunLevelState(),
                "Preventing illegal transition from %s -> %s", startState, endState);
            return startState;
        }

        private static final Set<State> startedStates = Arrays.stream(State.values())
            .filter(State::isRunLevelState)
            .filter(state -> state.direction(STARTED_SERVICES_UNCONFIGURED) != UP)
            .flatMap(state -> Stream.of(state, state.runLevel.up, state.runLevel.down))
            .filter(state -> state != null && state != STARTED_SERVICES_UNCONFIGURED.runLevel.down)
            .collect(Collectors.toSet());

        public boolean isStarted()
        {
            return startedStates.contains(this);
        }

        enum TransitionDirection
        {
            NONE,
            UP,
            DOWN
        }

        TransitionDirection direction(State endState)
        {
            if (endState.compareTo(this) > 0)
            {
                return UP;
            }
            else if (endState.compareTo(this) < 0)
            {
                return DOWN;
            }
            else
            {
                return NONE;
            }
        }

        /** Return the next transition state on the way to endState, iff the resultant transition
         *  ends at a state at or before endState AND the current state isn't already endState */
        public Optional<State> nextTransitionState(State endState)
        {
            Preconditions.checkArgument(runLevel != null,
                "Cannot get the next transition for non-runlevel state %s", this);

            if (direction(endState) == UP)
            {
                return runLevel.up().filter(transitionState -> transitionState.transition.direction(endState) != DOWN);
            }
            else if (direction(endState) == DOWN)
            {
                return runLevel.down().filter(transitionState -> transitionState.transition.direction(endState) != UP);
            }
            else
            {
                return Optional.empty();
            }
        }

        public Optional<Pair<Function<NodeGroup, CheckResourcesResult>, ActionStates>>
            nextResourceAction(State endState)
        {
            return nextTransitionState(endState)
                .flatMap(transitionState -> transitionState.transition.resourceAction
                    .map(action -> Pair.of(action,
                        new ActionStates(transitionState, transitionState.transition.next))));
        }

        public Optional<Pair<Function<NodeGroup, Boolean>, ActionStates>>
            nextBooleanAction(State endState)
        {
            return nextTransitionState(endState)
                .flatMap(transitionState -> transitionState.transition.booleanAction
                    .map(action -> Pair.of(action,
                        new ActionStates(transitionState, transitionState.transition.next))));
        }
    }

    public EnsembleCredentials getCredentials()
    {
        return credentials;
    }
}
