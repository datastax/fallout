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

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.util.ScopedLogger;

/**
 * Represents an API that can install and run software on nodes
 *
 * All operations must be idempotent
 * All operations are async
 */
public abstract class ConfigurationManager extends EnsembleComponent
    implements DebugInfoProvidingComponent, AutoCloseable
{
    private static final Logger classLogger = LoggerFactory.getLogger(ConfigurationManager.class);
    private ScopedLogger logger = ScopedLogger.getLogger(classLogger);
    private Ensemble ensemble;
    private FalloutConfiguration falloutConfiguration;
    private String instanceName;

    public void setLogger(Logger logger)
    {
        this.logger = ScopedLogger.getLogger(logger);
    }

    public ScopedLogger logger()
    {
        return logger;
    }

    public Ensemble getEnsemble()
    {
        return ensemble;
    }

    public void setEnsemble(Ensemble ensemble)
    {
        this.ensemble = ensemble;
    }

    public void setFalloutConfiguration(FalloutConfiguration configuration)
    {
        this.falloutConfiguration = configuration;
    }

    public <FC extends FalloutConfiguration> FC getFalloutConfiguration()
    {
        Preconditions.checkNotNull(falloutConfiguration);
        return (FC) falloutConfiguration;
    }

    @Override
    public void setInstanceName(String instanceName)
    {
        Preconditions.checkArgument(this.instanceName == null, "ConfigurationManager instance name already set");
        this.instanceName = instanceName;
    }

    @Override
    public String getInstanceName()
    {
        return instanceName;
    }

    /**
     * Returns the providers that will be added to each Node by this CM for the given NodeGroup properties
     *
     * @param nodeGroupProperties
     * @return the set of Providers to be installed on each Node
     */
    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of();
    }

    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of();
    }

    /**
     * Creates all necessary providers given the properties of the Node.
     */
    public boolean registerProviders(Node node)
    {
        return true;
    }

    /**
     * Removes all necessary providers given the properties of the Node.
     */
    public boolean unregisterProviders(Node node)
    {
        return true;
    }

    public boolean configureAndRegisterProviders(NodeGroup nodeGroup)
    {
        logger.info("Configuring nodegroup...");
        boolean configureSuccess = configureImpl(nodeGroup);
        if (!configureSuccess)
        {
            logger.error("Nodegroup configuration failed!");
            return false;
        }

        logger.info("Registering providers on nodegroup...");
        boolean registrationSuccess = nodeGroup.waitForAllNodes(this::registerProviders,
            "register providers");
        if (!registrationSuccess)
        {
            logger.error("Nodegroup provider registration failed!");
            return false;
        }
        return true;
    }

    protected boolean configureImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    protected boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    protected boolean startImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    protected boolean stopImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    public void summarizeInfo(InfoConsumer infoConsumer)
    {
        HashMap<String, Object> info = new HashMap<>();
        doSummarizeInfo(info::put);
        String component_prefix = prefix().replace("fallout.configuration.management", "configuration_manager");
        infoConsumer.accept(component_prefix.substring(0, component_prefix.length() - 1), info);
    }

    /**
     * Left empty so child classes aren't required to implement if they have nothing to add
     * @param infoConsumer
     */
    public void doSummarizeInfo(InfoConsumer infoConsumer)
    {

    }

    /**
     * Inspects the node group when its current state is unknown.  If a sensible state can't be derived,
     * returns the current state.
     */
    public final CompletableFuture<NodeGroup.State> checkState(NodeGroup nodeGroup)
    {
        return CompletableFuture.supplyAsync(() -> checkStateImpl(nodeGroup));
    }

    /** Override in subclasses if they can derive the node group state */
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.getState();
    }

    /**
     * Executes any actions which require the services are running in order to create artifacts.
     */
    public CompletableFuture<Boolean> prepareArtifacts(Node node)
    {
        return CompletableFuture.supplyAsync(() -> prepareArtifactsImpl(node));
    }

    protected boolean prepareArtifactsImpl(Node node)
    {
        return true;
    }

    /**
     * Makes sure all configured services copy their artifacts to the artifactsLocation
     */
    public CompletableFuture<Boolean> collectArtifacts(Node node)
    {
        return CompletableFuture.supplyAsync(() -> collectArtifactsImpl(node));
    }

    protected boolean collectArtifactsImpl(Node node)
    {
        return true;
    }

    @Override
    public void close()
    {
        // do nothing by default
    }
}
