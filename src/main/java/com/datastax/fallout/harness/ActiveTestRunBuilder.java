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
package com.datastax.fallout.harness;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.harness.artifact_checkers.SmartLogChecker;
import com.datastax.fallout.harness.checkers.NoErrorChecker;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.EnsembleCredentials;
import com.datastax.fallout.ops.FalloutPropertySpecs;
import com.datastax.fallout.ops.JobLoggers;
import com.datastax.fallout.ops.LocalFilesHandler;
import com.datastax.fallout.ops.MultiConfigurationManager;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyBasedComponent;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.LocalCommandExecutor;
import com.datastax.fallout.ops.commands.RejectableNodeCommandExecutor;
import com.datastax.fallout.ops.configmanagement.NoopConfigurationManager;
import com.datastax.fallout.ops.provisioner.LocalProvisioner;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.resources.server.TestResource;
import com.datastax.fallout.util.ComponentFactory;
import com.datastax.fallout.util.ServiceLoaderComponentFactory;

import static com.datastax.fallout.ops.ClusterNames.generateClusterName;
import static com.datastax.fallout.util.YamlUtils.loadYaml;

/**
 * ActiveTestRunBuilder to build an ActiveTestRun. Easiest way to build
 * an ActiveTestRun from test YAML while preserving invariants.
 */
public class ActiveTestRunBuilder
{
    public static final String NODE_COUNT_KEY = "node.count";

    private static final Logger logger = LoggerFactory.getLogger(ActiveTestRunBuilder.class);

    private ComponentFactory componentFactory = new ServiceLoaderComponentFactory();
    private CommandExecutor commandExecutor = new LocalCommandExecutor();

    private EnsembleBuilder ensembleBuilder;
    private boolean ensembleOwner = true;
    private String testRunName;
    private Path testRunArtifactPath;
    private FalloutConfiguration configuration;
    private TestRun testRun;

    private String workloadYaml;
    private String ensembleYaml;
    private Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker =
        (e) -> List.of(CompletableFuture.completedFuture(true));

    //Create a builder for each Ensemble group by default
    private final NodeGroupBuilder observerBuilder = NodeGroupBuilder.create();
    private final NodeGroupBuilder controllerBuilder = NodeGroupBuilder.create();
    private NodeGroupBuilder localBuilder;

    //Workload is both phases of modules and checkers
    private Workload workload;
    private TestRunAbortedStatusUpdater testRunStatusUpdater;

    private UserCredentials userCredentials;
    private JobLoggers loggers;

    public static ActiveTestRunBuilder create()
    {
        return new ActiveTestRunBuilder();
    }

    private ActiveTestRunBuilder()
    {
    }

    public ActiveTestRunBuilder withComponentFactory(ComponentFactory componentFactory)
    {
        if (componentFactory != null)
        {
            this.componentFactory = componentFactory;
        }
        return this;
    }

    public ActiveTestRunBuilder withCommandExecutor(CommandExecutor commandExecutor)
    {
        if (commandExecutor != null)
        {
            this.commandExecutor = commandExecutor;
        }
        return this;
    }

    private void validateYamlKeys(Map<String, Object> yamlMap, Set<String> validKeys, String logContext)
    {
        Set<String> yamlMapKeys = new HashSet<>();
        yamlMapKeys.addAll(yamlMap.keySet());
        yamlMapKeys.removeAll(validKeys);
        if (yamlMapKeys.size() > 0)
        {
            String exceptionMsg = "Found invalid yaml keys " + logContext + ": " + yamlMapKeys.toString() +
                ". Only valid keys are " + validKeys.toString();
            throw new InvalidConfigurationException(exceptionMsg);
        }
    }

    private void validateTopLevelYamlKeys(Map<String, Object> yamlMap, Set<String> validKeys)
    {
        validateYamlKeys(yamlMap, validKeys, "at the top level");
    }

    /**
     * Internal helper to build an Ensemble from YAML.
     * This takes in the whole test YAML and internally sets up the EnsembleBuilder to use for this test run.
     */
    @SuppressWarnings("unchecked")
    private void prepareEnsemble(String testYaml)
    {
        logger.trace("Preparing ensemble from yaml {}", testYaml);

        Map<String, Object> yamlMap = loadYaml(testYaml);
        validateTopLevelYamlKeys(yamlMap, ImmutableSet.of("ensemble", "workload"));
        prepareEnsemble(yamlMap);
    }

    private void prepareEnsemble(Map<String, Object> yamlMap)
    {
        if (yamlMap.containsKey("ensemble"))
        {
            Map<String, Object> ensembleMap = (Map) yamlMap.get("ensemble");
            //init or link each ensemble group
            for (Map.Entry<String, Object> entry : ensembleMap.entrySet())
            {
                String ensembleGroup = entry.getKey();
                Object ensembleValue = entry.getValue();
                switch (ensembleGroup.toLowerCase())
                {
                    case "servers":
                    case "clients":
                        List<Object> subGroups = (List<Object>) ensembleValue;
                        String subEnsembleGroup = "servers".equalsIgnoreCase(ensembleGroup) ? "server" : "client";
                        for (Object subEnsembleValue : subGroups)
                        {
                            readNodeGroup(subEnsembleGroup, subEnsembleValue);
                        }
                        break;
                    default:
                        readNodeGroup(ensembleGroup, ensembleValue);
                        break;
                }
            }
        }
        else
        {
            ensembleBuilder.withServerGroup(getGroupBuilder(Ensemble.Role.SERVER, "empty"));
            ensembleBuilder.withClientGroup(getGroupBuilder(Ensemble.Role.CLIENT, "empty"));
        }
    }

    private NodeGroupBuilder createLocalBuilder()
    {
        final LocalProvisioner provisioner = new LocalProvisioner();
        provisioner.setLocalCommandExecutor(commandExecutor);

        return NodeGroupBuilder.create()
            .withNodeCount(1)
            .withName("local")
            .withConfigurationManager(new NoopConfigurationManager())
            .withProvisioner(provisioner)
            .withNodeCommandExecutor(new RejectableNodeCommandExecutor(testRunStatusUpdater, provisioner))
            .withPropertyGroup(new WritablePropertyGroup());
    }

    private void readNodeGroup(String ensembleGroup, Object ensembleValue)
    {
        String linkGroup = null;
        // Special case links to another group
        if (ensembleValue instanceof String)
            linkGroup = (String) ensembleValue;

        NodeGroupBuilder ngBuilder;
        switch (ensembleGroup.toLowerCase())
        {
            case "server":
                ngBuilder = getGroupBuilder(Ensemble.Role.SERVER, linkGroup);
                ensembleBuilder.withServerGroup(ngBuilder);
                break;
            case "client":
                ngBuilder = getGroupBuilder(Ensemble.Role.CLIENT, linkGroup);
                ensembleBuilder.withClientGroup(ngBuilder);
                break;
            case "observer":
                ngBuilder = getGroupBuilder(Ensemble.Role.OBSERVER, linkGroup);
                ensembleBuilder.withObserverGroup(ngBuilder);
                break;
            case "controller":
                ngBuilder = getGroupBuilder(Ensemble.Role.CONTROLLER, linkGroup);
                ensembleBuilder.withControllerGroup(ngBuilder);
                break;
            default:
                throw new InvalidConfigurationException("Invalid ensemble group: " + ensembleGroup);
        }

        //If we only are linking to another group our job is done
        //start next group
        if (linkGroup != null)
        {
            ngBuilder.withAlias(ensembleGroup);
            logger.debug("Linking ensemble group {} to {}", ensembleGroup, linkGroup);
            return;
        }

        Map<String, Object> config = (Map) ensembleValue;
        validateYamlKeys(config, ImmutableSet.of("name", NODE_COUNT_KEY, "runlevel", "runlevel.final", "provisioner",
            "configuration_manager", "mark_for_reuse", "local_files"), "below nodegroup " + ensembleGroup);
        ngBuilder.withName("" + config.getOrDefault("name", ensembleGroup.toLowerCase()));

        requiredKey(config, NODE_COUNT_KEY, ensembleGroup);
        Integer nodeCount = (Integer) config.get(NODE_COUNT_KEY);
        if (nodeCount == null || nodeCount < 1 || nodeCount > 1024)
        {
            throw new InvalidConfigurationException(String.format(
                "Invalid %s for ensemble group %s: %d", NODE_COUNT_KEY, ensembleGroup, nodeCount));
        }
        ngBuilder.withNodeCount(nodeCount);

        WritablePropertyGroup propertyGroup = new WritablePropertyGroup(); // shared between nodegroup, provisioner and CM

        String runLevel = (String) config.get("runlevel");
        if (runLevel != null)
        {
            NodeGroup.State launchRunLevel = NodeGroup.State.valueOf(runLevel);
            propertyGroup.put(FalloutPropertySpecs.launchRunLevelPropertySpec.name(), launchRunLevel);
        }

        String finalRunLevelVal = (String) config.get("runlevel.final");
        Boolean mfr = (Boolean) config.get("mark_for_reuse");

        if (finalRunLevelVal != null && mfr != null)
        {
            throw new InvalidConfigurationException("Cannot set mark_for_reuse and runlevel.final at the same time.");
        }
        else if (finalRunLevelVal != null)
        {
            ngBuilder.withFinalRunLevel(Optional.of(NodeGroup.State.valueOf(finalRunLevelVal)));
        }
        else if (mfr != null && mfr)
        {
            ngBuilder.withFinalRunLevel(Optional.empty());
        }

        if (testRun != null)
        {
            URI testRunUrl = URI.create(configuration.getExternalUrl())
                .resolve(TestResource.uriForShowTestRunArtifacts(testRun));
            propertyGroup.put(FalloutPropertySpecs.testRunUrl.name(), testRunUrl.toString());
            propertyGroup.put(FalloutPropertySpecs.testRunId.name(), testRun.getTestRunId().toString());
        }

        LocalFilesHandler localFilesHandler = new LocalFilesHandler(List.of());
        List<Map<String, Object>> localFiles = (List<Map<String, Object>>) config.get("local_files");
        if (localFiles != null)
        {
            localFilesHandler = new LocalFilesHandler(localFiles);
            ngBuilder.withLocalFilesHandler(localFilesHandler);
        }

        //Handle provisioner spec.
        Map<String, Object> provisionerMap;
        Object provisionerValue = config.get("provisioner");

        if (provisionerValue instanceof String)
        {
            provisionerMap = new HashMap<>();
            provisionerMap.put("name", provisionerValue);
        }
        else if (provisionerValue instanceof Map)
        {
            provisionerMap = (Map) provisionerValue;
        }
        else
        {
            throw new InvalidConfigurationException("Missing provisioner under ensemble group: " + ensembleGroup);
        }

        validateYamlKeys(provisionerMap, ImmutableSet.of("name", "properties"),
            "below provisioner of nodegroup " + ensembleGroup);
        String provisionerName = (String) provisionerMap.get("name");
        if (provisionerName == null)
            throw new InvalidConfigurationException("Provisioner name is missing for ensemble group: " + ensembleGroup);
        String provisionerAlias = ensembleGroup + "-" + provisionerName;
        Provisioner provisioner = createPropertyBasedComponent(Provisioner.class, provisionerAlias, provisionerName,
            (Map) provisionerMap.get("properties"), propertyGroup);
        provisioner.setLocalCommandExecutor(commandExecutor);
        ngBuilder.withProvisioner(provisioner);
        ngBuilder.withNodeCommandExecutor(new RejectableNodeCommandExecutor(testRunStatusUpdater, provisioner));

        //Handle config management spec
        Object configManagerValue = config.get("configuration_manager");
        List<Object> cmValues;
        if (configManagerValue == null)
        {
            cmValues = Lists.newArrayList("noop");
        }
        else if (configManagerValue instanceof List)
        {
            cmValues = (List) configManagerValue;
        }
        else
        {
            cmValues = Lists.newArrayList(configManagerValue);
        }

        List<ConfigurationManager> cmList = new ArrayList<>(cmValues.size());
        for (Object cmValue : cmValues)
        {
            Map<String, Object> configManagerMap;
            if (cmValue instanceof String)
            {
                configManagerMap = new HashMap<>();
                configManagerMap.put("name", cmValue);
            }
            else if (cmValue instanceof Map)
            {
                configManagerMap = (Map) cmValue;
            }
            else
            {
                throw new InvalidConfigurationException(
                    "invalid configuration_manager settings ensemble group: " + ensembleGroup);
            }

            validateYamlKeys(configManagerMap, ImmutableSet.of("name", "properties"),
                "below configuration_manager of nodegroup " + ensembleGroup);
            String configManagerName = (String) configManagerMap.get("name");
            if (configManagerName == null)
                throw new InvalidConfigurationException(
                    "invalid configuration_manager settings for ensemble group:" + ensembleGroup);

            String cmAlias = ensembleGroup + "-" + configManagerName;
            ConfigurationManager cm = createPropertyBasedComponent(ConfigurationManager.class, cmAlias,
                configManagerName, (Map) configManagerMap.get("properties"), propertyGroup);
            cm.setFalloutConfiguration(configuration);
            cmList.add(cm);
        }

        if (cmList.isEmpty())
            throw new InvalidConfigurationException(
                "Missing configuration_manager under ensemble group: " + ensembleGroup);

        Set<Class<? extends Provider>> ngAvailableProviders = new HashSet<>();
        ngAvailableProviders.addAll(provisioner.getAvailableProviders(propertyGroup));
        ngAvailableProviders.addAll(localFilesHandler.getAvailableProviders());

        ConfigurationManager cm = cmList.size() > 1 ?
            new MultiConfigurationManager(cmList, ngAvailableProviders, propertyGroup) :
            cmList.get(0);

        ngBuilder.withConfigurationManager(cm);

        //Finally set the property group
        ngBuilder.withPropertyGroup(propertyGroup);
    }

    private NodeGroupBuilder getOrCreateLocalBuilder()
    {
        if (localBuilder == null)
        {
            localBuilder = createLocalBuilder();
        }
        return localBuilder;
    }

    private NodeGroupBuilder getGroupBuilder(Ensemble.Role type, String linkGroup)
    {
        if (linkGroup != null)
        {
            switch (linkGroup.toLowerCase())
            {
                case "observer":
                    return observerBuilder;
                case "controller":
                    return controllerBuilder;
                case "local":
                case "none":
                case "empty":
                    return getOrCreateLocalBuilder();
                default:
                    Optional<NodeGroupBuilder> serverMatch = ensembleBuilder.findServerGroup(linkGroup);
                    if (serverMatch.isPresent())
                    {
                        return serverMatch.get();
                    }
                    Optional<NodeGroupBuilder> clientMatch = ensembleBuilder.findClientGroup(linkGroup);
                    if (clientMatch.isPresent())
                    {
                        return clientMatch.get();
                    }
                    throw new InvalidConfigurationException(
                        "Unknown group alias used in ensemble group " + type + " : " + linkGroup);
            }
        }

        switch (type)
        {
            case SERVER:
            case CLIENT:
                return NodeGroupBuilder.create();
            case OBSERVER:
                return observerBuilder;
            case CONTROLLER:
                return controllerBuilder;
            default:
                throw new IllegalStateException("Unknown ensemble role " + type);
        }
    }

    private static void requiredKey(Map<String, Object> yamlMap, String key, String ensembleGroup)
    {
        if (!yamlMap.containsKey(key))
            throw new InvalidConfigurationException("Missing required key for ensemble group '" +
                (ensembleGroup == null ? "" : ensembleGroup) + "': " + key);
    }

    /**
     * Associates an Ensemble created from testYaml with this ActiveTestRunBuilder
     * @param testYaml
     * @return self
     */
    public ActiveTestRunBuilder withEnsembleFromYaml(String testYaml)
    {
        Preconditions.checkState(ensembleBuilder == null,
            "Cannot provide an ensembleYaml if an EnsembleBuilder is already set!");
        ensembleYaml = testYaml;
        return this;
    }

    /**
     * Associates existing ensemblebuilder with this ActiveTestRunBuilder
     *
     * @param ensembleBuilder an existing ensemblebuilder
     * @param testOwnsEnsemble indicates if this test runner should take care of tearing down ensemble when finished
     * @return
     */
    public ActiveTestRunBuilder withEnsembleBuilder(EnsembleBuilder ensembleBuilder, boolean testOwnsEnsemble)
    {
        Preconditions.checkState(ensembleYaml == null,
            "Cannot provide an EnsembleBuilder if an ensembleYaml is already set!");
        this.ensembleBuilder = ensembleBuilder;
        this.ensembleOwner = testOwnsEnsemble;

        return this;
    }

    public ActiveTestRunBuilder withTestRunArtifactPath(Path testRunArtifactPath)
    {
        if (testRunArtifactPath != null)
            this.testRunArtifactPath = testRunArtifactPath;
        return this;
    }

    public ActiveTestRunBuilder withFalloutConfiguration(FalloutConfiguration configuration)
    {
        if (configuration != null)
            this.configuration = configuration;
        return this;
    }

    public ActiveTestRunBuilder destroyEnsembleAfterTest(boolean destroy)
    {
        this.ensembleOwner = destroy;
        return this;
    }

    /**
     * Internal helper method to prepare a workloadfrom YAML.
     * If the workload body is a class name, we treat it as a Jepsen test.
     *
     * @param testYaml
     * @return
     */
    @SuppressWarnings("unchecked")
    private Workload prepareWorkload(String testYaml)
    {
        logger.debug("Preparing workload from yaml {}", testYaml);

        Map<String, Object> yamlMap = loadYaml(testYaml);
        validateTopLevelYamlKeys(yamlMap, ImmutableSet.of("ensemble", "workload"));
        return prepareWorkload(yamlMap);
    }

    private Workload prepareWorkload(Map<String, Object> yamlMap)
    {
        Object workload = yamlMap.get("workload");
        if (workload instanceof String)
        {
            throw new InvalidConfigurationException("Jepsen test names not yet supported");
        }
        if (!(workload instanceof Map))
        {
            throw new InvalidConfigurationException("Workload is not a Jepsen test name or Fallout workload");
        }
        Map workloadMap = (Map) workload;
        validateYamlKeys(workloadMap, ImmutableSet.of("phases", "checkers", "artifact_checkers"), "below workload");
        List<Map<String, Object>> rawYamlForPhases = (List<Map<String, Object>>) workloadMap.get("phases");
        Map<String, Object> yamlCheckers = (Map<String, Object>) workloadMap.get("checkers");
        Map<String, Object> yamlArtifactCheckers = (Map<String, Object>) workloadMap.get("artifact_checkers");

        if (rawYamlForPhases == null || rawYamlForPhases.isEmpty())
        {
            throw new InvalidConfigurationException("Phases section required");
        }

        List<Phase> topLevelPhases = new ArrayList<>();
        for (Map<String, Object> items : rawYamlForPhases)
        {
            topLevelPhases.add(parsePhase(String.format("top-level-%d", rawYamlForPhases.indexOf(items)), items));
        }
        logger.debug(topLevelPhases.toString());

        List<String> duplicateModuleAliases = findDuplicateModuleAliases(topLevelPhases);
        if (!duplicateModuleAliases.isEmpty())
        {
            throw new InvalidConfigurationException("Duplicate module or subphase aliases: " +
                String.join(", ", duplicateModuleAliases));
        }

        Map<String, Checker> checkers = parseCheckers(yamlCheckers);
        Map<String, ArtifactChecker> artifactCheckers = parseArtifactCheckers(yamlArtifactCheckers);

        return new Workload(topLevelPhases, checkers, artifactCheckers);
    }

    private Stream<String> moduleAliases(Collection<Phase> phases)
    {
        return phases.stream()
            .flatMap(phase -> Stream.concat(
                phase.getTopLevelModules().keySet().stream(),
                phase.getSubPhases().values().stream()
                    .flatMap(this::moduleAliases)));
    }

    private List<String> findDuplicateModuleAliases(List<Phase> phases)
    {
        Set<String> moduleNamesSeen = new HashSet<>();
        return moduleAliases(phases).filter(name -> !moduleNamesSeen.add(name)).collect(Collectors.toList());
    }

    /**
     * Guy Bolton King:
         1. A phase is a dictionary, where each key contains either
         a) a module definition, or
         b) a list of phases (i.e. dictionaries)
         2. Phases in a list are executed serially
         3. Modules and subphases in the same phase are executed concurrently
     */
    private Phase parsePhase(String name, Map<String, Object> yaml)
    {
        Map<String, Module> phaseModules = new HashMap<>();
        Map<String, List<Phase>> phaseLists = new HashMap<>();
        for (Map.Entry<String, Object> entry : yaml.entrySet())
        {
            String alias = entry.getKey();
            if (entry.getValue() instanceof List)
            {
                List<Phase> subPhases = new ArrayList<>();
                int i = 0;
                for (Object subPhase : (List) entry.getValue())
                {
                    try
                    {
                        subPhases.add(parsePhase(String.format("%s-%d", alias, i++), (Map<String, Object>) subPhase));
                    }
                    catch (ClassCastException e)
                    {
                        throw new InvalidConfigurationException("Phase must be a map: " + subPhase);
                    }
                }
                phaseLists.put(alias, subPhases);
            }
            else if (!(entry.getValue() instanceof Map))
            {
                throw new InvalidConfigurationException("Missing module information under workload step: " + alias);
            }
            else
            {
                Module moduleInstance = parseWorkloadComponent("module", Module.class, entry);
                phaseModules.put(moduleInstance.getInstanceName(), moduleInstance);
            }
        }
        return new Phase(name, phaseLists, phaseModules);
    }

    private Map<String, Checker> parseCheckers(Map<String, Object> yamlCheckers)
    {
        Map<String, Checker> res = new HashMap<>();
        if (yamlCheckers != null)
        {
            for (Map.Entry<String, Object> entry : yamlCheckers.entrySet())
            {
                Checker checkerInstance = parseWorkloadComponent("checker", Checker.class, entry);
                res.put(checkerInstance.getInstanceName(), checkerInstance);
            }
        }
        return res;
    }

    private Map<String, ArtifactChecker> parseArtifactCheckers(Map<String, Object> yamlArtifactCheckers)
    {
        Map<String, ArtifactChecker> res = new HashMap<>();
        if (yamlArtifactCheckers != null)
        {
            for (Map.Entry<String, Object> entry : yamlArtifactCheckers.entrySet())
            {
                ArtifactChecker artifactCheckerInstance =
                    parseWorkloadComponent("artifact_checker", ArtifactChecker.class, entry);
                res.put(artifactCheckerInstance.getInstanceName(), artifactCheckerInstance);
            }
        }
        return res;
    }

    private <T extends WorkloadComponent> T parseWorkloadComponent(String typeKey, Class<T> clazz,
        Map.Entry<String, Object> entry)
    {
        String alias = entry.getKey();
        if (!(entry.getValue() instanceof Map))
        {
            throw new InvalidConfigurationException(
                "Missing " + typeKey + " information under workload step: " + alias);
        }

        Set<String> allowedKeys = Sets.newHashSet(typeKey, "properties");
        Map<String, Object> componentData = (Map) entry.getValue();
        validateYamlKeys(componentData, allowedKeys, "below " + typeKey + " '" + alias + "'");
        if (!componentData.containsKey(typeKey))
        {
            throw new InvalidConfigurationException("Missing " + typeKey + " field under workload step: " + alias);
        }

        String typeName = (String) componentData.get(typeKey);
        Object propertyObj = componentData.get("properties");
        if (propertyObj != null && !(propertyObj instanceof Map))
        {
            throw new InvalidConfigurationException(
                "Invalid property map found for " + typeKey + " " + typeName + " in step " + alias);
        }
        Map<String, Object> propertyMap = (Map<String, Object>) propertyObj;
        WritablePropertyGroup componentProps = new WritablePropertyGroup();
        T componentInstance = createPropertyBasedComponent(clazz, alias, typeName, propertyMap, componentProps);
        componentInstance.setProperties(componentProps);
        return componentInstance;
    }

    private <T extends PropertyBasedComponent> T createPropertyBasedComponent(Class<T> clazz, String alias, String name,
        Map<String, Object> propertyMap, WritablePropertyGroup propertyGroup)
    {
        T instance = componentFactory.create(clazz, name);
        if (instance == null)
        {
            throw new InvalidConfigurationException("No implementation found with name " + name + " in entry " + alias);
        }

        if (configuration.getIsSharedEndpoint() && instance.disabledWhenShared())
        {
            throw new InvalidConfigurationException(String.format("Cannot use %s on a shared endpoint!", name));
        }

        logger.debug("Created instance with description " + instance.description() + " for component with name " +
            name + " under entry " + alias);

        if (propertyMap != null)
        {
            String prefix = instance.prefix();
            for (Map.Entry<String, Object> configEntry : propertyMap.entrySet())
            {
                String key = prefix + configEntry.getKey();
                if (propertyGroup.hasProperty(key))
                {
                    throw new InvalidConfigurationException("Duplicate property key: " + key);
                }
                propertyGroup.put(key, configEntry.getValue());
            }
        }
        instance.setInstanceName(alias);
        return instance;
    }

    /**
     * Associates a workload built from the provided testYaml with this instance
     * @param testYaml
     * @return this
     */
    public ActiveTestRunBuilder withWorkloadFromYaml(String testYaml)
    {
        workloadYaml = testYaml;
        return this;
    }

    /**
     * Associates existing workload with this ActiveTestRunBuilder
     * @return this
     */
    public ActiveTestRunBuilder withWorkload(Workload workload)
    {
        this.workload = workload;
        return this;
    }

    public ActiveTestRunBuilder withTestRunStatusUpdater(TestRunAbortedStatusUpdater testRunStatusUpdater)
    {
        this.testRunStatusUpdater = testRunStatusUpdater;
        return this;
    }

    /**
     * Sets the name of this testrun. Used to store artifacts locally by testrunname
     * @param testRunName
     * @return this
     */
    public ActiveTestRunBuilder withTestRunName(String testRunName)
    {
        this.testRunName = testRunName;
        return this;
    }

    public ActiveTestRunBuilder withUserCredentials(UserCredentials userCredentials)
    {
        this.userCredentials = userCredentials;
        return this;
    }

    public ActiveTestRunBuilder withTestRun(TestRun testRun)
    {
        this.testRun = testRun;
        return this;
    }

    public ActiveTestRunBuilder withLoggers(JobLoggers loggers)
    {
        this.loggers = loggers;
        return this;
    }

    public ActiveTestRunBuilder withResourceChecker(
        Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker)
    {
        this.resourceChecker = resourceChecker;
        return this;
    }

    /**
     * Checks if all modules used in the test support the installed product.
     * We assume that if no product is installed, the test is valid.
     * We assume all server groups install the same product.
     * @param modules all modules in the workload
     * @param serverGroup a server groups in the ensemble
     * @return true if all modules support the installed product, else false
     */
    private void validateProduct(List<Module> modules, NodeGroup serverGroup, ValidationResult validationResult)
    {
        Optional<Product> installedProduct = serverGroup.getConfigurationManager().product(serverGroup);

        if (!installedProduct.isPresent())
        {
            logger
                .warn("No product installed. We are not going to validate if the modules used are legal. You either " +
                    "meant to install a product and messed up, or didn't and you know what you're doing.");
            return;
        }

        for (Module module : modules)
        {
            // If the installed product IS NOT supported by a single module, we fail.
            if (!module.getSupportedProducts().contains(installedProduct.get()))
            {
                validationResult.addError(
                    String.format("Installed product is %s, and you are using module %s which does not support it.",
                        installedProduct.get(), module.name()));
            }
        }
    }

    private void validateEnsemblePropertySpecs(Ensemble ensemble, ValidationResult validationResult)
    {
        Set<String> seenExplicitClusterNames = new HashSet<>();
        final User user = userCredentials.owner;

        for (NodeGroup g : ensemble.getUniqueNodeGroupInstances())
        {
            Optional<String> explicitClusterName = g.explicitClusterName();
            if (explicitClusterName.isPresent())
            {
                String clusterName = explicitClusterName.get();
                if (seenExplicitClusterNames.contains(clusterName))
                {
                    validationResult.addError(
                        String.format("Multiple nodegroups can not use the same explicit cluster name: %s",
                            clusterName));
                }
                seenExplicitClusterNames.add(clusterName);
            }
            else
            {
                putGeneratedClusterName(g, g.getProvisioner().generateClusterName(
                    g, testRunName, testRun, user));
            }

            while (true)
            {
                try
                {
                    List<PropertySpec> combinedSpecs = ImmutableList.<PropertySpec>builder()
                        .addAll(g.getProvisioner().getPropertySpecs())
                        .addAll(g.getConfigurationManager().getPropertySpecs())
                        .build();
                    g.getProperties().validateFull(combinedSpecs);

                    break;
                }
                catch (PropertySpec.ValidationException e)
                {
                    if (e.failedSpecs.isEmpty())
                    {
                        // general validation problem
                        validationResult.addError(String.format("General Validation Exception: %s", e.getMessage()));
                        break;
                    }
                    if (e.failedSpecs.size() > 1)
                    {
                        throw new RuntimeException(
                            "PropertyGroup::validateFull should never return multiple failed specs. If you're in the process of changing that, please update this catch block and the one in ClusterResource.java.");
                    }

                    PropertySpec<?> failedSpec = e.failedSpecs.get(0);
                    List<Optional<String>> aliases = Arrays.asList(failedSpec.alias(), failedSpec.deprecatedName());
                    boolean found = false;
                    for (Optional<String> alias : aliases)
                    {
                        //Add in any aliased information and retry
                        if (alias.isPresent() && g.getProperties().get(alias.get()) == null)
                        {
                            if (alias.get().equals(FalloutPropertySpecs.generatedClusterNamePropertySpec.name()))
                            {
                                putGeneratedClusterName(g,
                                    generateClusterName(g, testRunName, testRun, userCredentials.owner));
                                found = true;
                                break;
                            }
                        }
                    }

                    if (found)
                        continue;

                    validationResult.addError(String.format("NodeGroup %s had validation exception: %s",
                        g.getId(), e.getMessage()));
                    break; // exit loop since this validation error is not recoverable by re-trying
                }
            }
            Provisioner provisioner = g.getProvisioner();
            try
            {
                provisioner.validateProperties(g.getProperties());
            }
            catch (PropertySpec.ValidationException e)
            {
                validationResult.addError(String.format("Provisioner %s had validation exception: %s",
                    provisioner.getInstanceName(), e.getMessage()));
            }
            ConfigurationManager cfgMgr = g.getConfigurationManager();
            try
            {
                cfgMgr.validateProperties(g.getProperties());
            }
            catch (PropertySpec.ValidationException e)
            {
                validationResult.addError(String.format("ConfigurationManager %s had validation exception: %s",
                    cfgMgr.getInstanceName(), e.getMessage()));
            }
        }
    }

    private void putGeneratedClusterName(NodeGroup nodeGroup, String clusterName)
    {
        nodeGroup.getWritableProperties()
            .put(FalloutPropertySpecs.generatedClusterNamePropertySpec.name(), clusterName);
    }

    private void validateWorkLoad(ValidationResult validationResult)
    {
        for (Module module : workload.getAllModules())
        {
            try
            {
                module.validateProperties(module.getProperties());
            }
            catch (PropertySpec.ValidationException e)
            {
                validationResult.addError(String.format("Module %s had validation exception: %s",
                    module.getInstanceName(), e.getMessage()));
            }
        }
        for (Checker checker : workload.getCheckers().values())
        {
            try
            {
                checker.validateProperties(checker.getProperties());
            }
            catch (PropertySpec.ValidationException e)
            {
                validationResult.addError(String.format("Checker %s had validation exception: %s",
                    checker.getInstanceName(), e.getMessage()));
            }
        }
        for (ArtifactChecker artifactChecker : workload.getArtifactCheckers().values())
        {
            try
            {
                artifactChecker.validateProperties(artifactChecker.getProperties());
            }
            catch (PropertySpec.ValidationException e)
            {
                validationResult.addError(String.format("ArtifactChecker %s had validation exception: %s",
                    artifactChecker.getInstanceName(), e.getMessage()));
            }
        }
    }

    private void validateProviders(Ensemble ensemble, ValidationResult validationResult)
    {
        try
        {
            Set<Class<? extends Provider>> providersNeeded = workload.getAllModules()
                .stream()
                .map(Module::getRequiredProviders)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

            Set<Class<? extends Provider>> providersAvailable = ensemble.getUniqueNodeGroupInstances().stream()
                .map(NodeGroup::getAvailableProviders)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

            Set<Class<? extends Provider>> missingProviders = new HashSet<>();

            for (Class<? extends Provider> provider : providersNeeded)
            {
                if (providersAvailable.stream()
                    .noneMatch(p -> provider.isAssignableFrom(p) || p == provider))
                {
                    missingProviders.add(provider);
                }
            }

            if (!missingProviders.isEmpty())
            {
                validationResult.addError(
                    String.format("The following providers are required by your test, but were not installed: %s",
                        missingProviders));
            }
        }
        catch (PropertySpec.ValidationException e)
        {
            validationResult.addError(String.format("Validation exception: %s", e.getMessage()));
        }
    }

    public Ensemble buildEnsemble()
    {
        try (ValidationResult validationResult = new ValidationResult())
        {
            return buildEnsemble(validationResult);
        }
    }

    private Ensemble buildEnsemble(ValidationResult validationResult)
    {
        if (ensembleYaml != null)
        {
            ensembleBuilder = new EnsembleBuilder();
            prepareEnsemble(ensembleYaml);
        }

        Ensemble ensemble = ensembleBuilder
            .withTestRunId(testRunName)
            .withCredentials(new EnsembleCredentials(userCredentials, configuration))
            .withDefaultControllerGroup(createLocalBuilder())
            .withDefaultObserverGroup(createLocalBuilder())
            .withTestRunAbortedStatus(testRunStatusUpdater)
            .withLoggers(loggers)
            .build(testRunArtifactPath);

        validateEnsemblePropertySpecs(ensemble, validationResult);

        return ensemble;
    }

    /**
     * Creates an ActiveTestRun that can be executed
     * @return ActiveTestRun
     */
    public ActiveTestRun build()
    {
        Preconditions.checkArgument(testRunName != null, "testRunName is missing");
        Preconditions.checkArgument(ensembleBuilder != null || ensembleYaml != null, "Ensemble is missing");
        Preconditions.checkArgument(workload != null || workloadYaml != null, "Workload is missing");
        Preconditions.checkArgument(testRunStatusUpdater != null, "TestRunStatusUpdater is missing");
        Preconditions.checkArgument(loggers != null, "loggers is missing");
        Preconditions.checkArgument(resourceChecker != null, "resourceChecker is missing");

        Ensemble ensemble;

        try (ValidationResult validationResult = new ValidationResult())
        {
            ensemble = buildEnsemble(validationResult);

            if (workloadYaml != null)
            {
                workload = prepareWorkload(workloadYaml);
            }

            String implicitCheckerPrefix = "fallout-";
            // Add implicit NoErrorChecker
            workload.addChecker(implicitCheckerPrefix + "no_error", new NoErrorChecker());

            // If testRun is not passed, we must be in a unit test.
            if (testRun != null)
            {
                // Add implicit SmartLogChecker
                workload.addArtifactChecker(implicitCheckerPrefix + "smart_log", new SmartLogChecker(testRun));
            }
            else
            {
                logger.warn("Test Run not passed to ActiveTestRun, no attempt to parse logs will be made.");
            }

            workload.setLoggers(loggers);

            for (NodeGroup serverGroup : ensemble.getServerGroups())
            {
                validateProduct(workload.getAllModules(), serverGroup, validationResult);
            }

            validateWorkLoad(validationResult);
            validateProviders(ensemble, validationResult);
        }

        return new ActiveTestRun(ensemble, workload, testRunStatusUpdater, ensembleOwner, testRunArtifactPath,
            resourceChecker);
    }

    private static class ValidationResult implements AutoCloseable
    {
        private List<String> errors = new ArrayList<>();

        public void addError(String error)
        {
            errors.add(error);
        }

        @Override
        public void close()
        {
            if (!errors.isEmpty())
            {
                throw new InvalidConfigurationException(
                    String.format("Your test failed validation for the following reasons:\n%s",
                        String.join("\n", errors)));
            }
        }
    }

}
