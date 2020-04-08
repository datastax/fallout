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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.slf4j.Logger;

import com.datastax.fallout.FalloutVersion;
import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.DebugInfoProvidingComponent;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.FalloutPropertySpecs;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.core.TestRun;

/**
 * A class with all the information necessary to setup an {@link Ensemble} and run a {@link Workload} in the
 * Jepsen test harness.
 */
public class ActiveTestRun
{
    private final Ensemble ensemble;
    private final Workload workload;
    private final TestRunAbortedStatusUpdater testRunStatusUpdater;
    private final boolean ensembleOwner;
    private final Path testRunArtifactPath;
    private final Logger logger;
    private final Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker;

    private Optional<TestResult> result = Optional.empty();

    ActiveTestRun(Ensemble ensemble, Workload workload,
        TestRunAbortedStatusUpdater testRunStatusUpdater,
        boolean ensembleOwner, Path testRunArtifactPath,
        Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker)
    {
        this.ensemble = ensemble;
        this.workload = workload;
        this.testRunStatusUpdater = testRunStatusUpdater;
        this.ensembleOwner = ensembleOwner;
        this.testRunArtifactPath = testRunArtifactPath;
        this.logger = ensemble.getControllerGroup().logger();
        this.resourceChecker = resourceChecker;

        try
        {
            validate();
        }
        catch (Exception e)
        {
            throw new InvalidConfigurationException(e);
        }
    }

    private void validate()
    {
        boolean allComponentsHaveValidPrefixes = true;
        // do prefix-checking of Provisioners' and ConfigurationManagers' PropertySpecs first
        for (NodeGroup ng : ensemble.getUniqueNodeGroupInstances())
        {
            boolean currComponentHasValidPrefixes = ng.getProvisioner().validatePrefixes(logger);
            allComponentsHaveValidPrefixes = allComponentsHaveValidPrefixes && currComponentHasValidPrefixes;
            currComponentHasValidPrefixes = ng.getConfigurationManager().validatePrefixes(logger);
            allComponentsHaveValidPrefixes = allComponentsHaveValidPrefixes && currComponentHasValidPrefixes;
        }
        // do prefix-checking of Checkers' PropertySpecs
        for (Map.Entry<String, Checker> entry : workload.getCheckers().entrySet())
        {
            boolean currComponentHasValidPrefixes = entry.getValue().validatePrefixes(logger);
            allComponentsHaveValidPrefixes = allComponentsHaveValidPrefixes && currComponentHasValidPrefixes;
        }
        // do prefix-checking of Phases' PropertySpecs
        for (Phase phase : workload.getPhases())
        {
            for (Map.Entry<String, Module> entry : phase.getAllModulesRecursively().entrySet())
            {
                boolean currComponentHasValidPrefixes = entry.getValue().validatePrefixes(logger);
                allComponentsHaveValidPrefixes = allComponentsHaveValidPrefixes && currComponentHasValidPrefixes;
            }
        }
        // no need to validate PropertySpecs fully, in the case that prefixes already do not match up
        if (!allComponentsHaveValidPrefixes)
        {
            throw new IllegalStateException(
                "Some PropertyBasedComponents have PropertySpecs whose prefixes do not match up with that of the PropertyBasedComponent. Please check messages in fallout-errors.log");
        }
        // prefixes all were valid, time to fully validate propertySpecs of our components
        for (NodeGroup ng : ensemble.getUniqueNodeGroupInstances())
        {
            List<PropertySpec> combinedSpecs = ImmutableList.<PropertySpec>builder()
                .addAll(ng.getProvisioner().getPropertySpecs())
                .addAll(ng.getConfigurationManager().getPropertySpecs())
                .build();
            ng.getProperties().validateFull(combinedSpecs);
        }

        Stream
            .concat(
                workload.getPhases().stream()
                    .flatMap(phase -> phase.getAllModulesRecursively().entrySet().stream()),
                Stream.concat(
                    workload.getCheckers().entrySet().stream(),
                    workload.getArtifactCheckers().entrySet().stream()))
            .forEach(entry -> entry.getValue().getProperties().validateFull(entry.getValue().getPropertySpecs()));

        // determine if all Configuration Manager dependencies are met
        for (NodeGroup ng : ensemble.getUniqueNodeGroupInstances())
        {
            Set<Class<? extends Provider>> availableProviders = ng.getAvailableProviders();

            Set<Class<? extends Provider>> requiredProviders =
                ng.getConfigurationManager().getRequiredProviders(ng.getProperties());
            HashSet<Class<? extends Provider>> missingProviders =
                new HashSet(Sets.difference(requiredProviders, availableProviders));

            for (Class<? extends Provider> missing : new HashSet<>(missingProviders))
            {
                if (availableProviders.stream().anyMatch(missing::isAssignableFrom))
                {
                    missingProviders.remove(missing);
                }
            }

            if (!missingProviders.isEmpty())
            {
                throw new IllegalStateException(
                    String.format(
                        "Providers required by the %s Configuration Manager were not found for NodeGroup %s. Available: %s\tRequired: %s\tMissing: %s",
                        ng.getConfigurationManager().name(), ng.getName(), availableProviders, requiredProviders,
                        missingProviders));
            }
        }
    }

    @VisibleForTesting
    public Ensemble getEnsemble()
    {
        return ensemble;
    }

    @VisibleForTesting
    TestRun.State getTestRunState()
    {
        return testRunStatusUpdater.getCurrentState();
    }

    @VisibleForTesting
    CheckResourcesResult checkResources()
    {
        if (testRunStatusUpdater.hasBeenAborted())
        {
            return CheckResourcesResult.FAILED;
        }

        final CheckResourcesResult checkResourcesResult = doCheckResources();

        if (testRunStatusUpdater.hasBeenAborted())
        {
            return CheckResourcesResult.FAILED;
        }

        return checkResourcesResult;
    }

    private CheckResourcesResult doCheckResources()
    {
        setTestRunState(TestRun.State.CHECKING_RESOURCES);

        Set<ResourceRequirement> resourceRequirements = ensemble.getResourceRequirements();

        final List<CompletableFuture<Boolean>> resChecks = resourceChecker.apply(ensemble);

        try
        {
            boolean allResourcesAvailable = Utils.waitForAll(resChecks);

            if (!allResourcesAvailable)
            {
                failTestTemporarily(String.format("Insufficient resources to run test (resources required: %s); " +
                    "will try again shortly", resourceRequirements));
            }

            return allResourcesAvailable ? CheckResourcesResult.AVAILABLE : CheckResourcesResult.UNAVAILABLE;
        }
        catch (Throwable e)
        {
            logger.error("Unexpected exception when checking resources", e);
            return CheckResourcesResult.FAILED;
        }
    }

    private void failTestIfCheckResourcesDidNotSucceed(CheckResourcesResult checkResourcesResult, String stage)
    {
        switch (checkResourcesResult)
        {
            case UNAVAILABLE:
                failTestTemporarily("Resources unavailable when " + stage + " ensemble for testrun, check logs");
            case FAILED:
                failTest("Error " + stage + " ensemble for testrun, check logs");
        }
    }

    @VisibleForTesting
    Optional<TestResult> getResult()
    {
        return result;
    }

    private CheckResourcesResult transitionEnsembleStateUpwards(Optional<NodeGroup.State> maximumState)
    {
        List<CompletableFuture<CheckResourcesResult>> futures = new ArrayList<>();

        for (NodeGroup nodeGroup : ensemble.getUniqueNodeGroupInstances())
        {
            NodeGroup.State requiredState =
                FalloutPropertySpecs.launchRunLevelPropertySpec.value(nodeGroup);
            requiredState = NodeGroup.State.values()[Math.min(maximumState.orElse(requiredState).ordinal(),
                requiredState.ordinal())];

            futures.add(nodeGroup.transitionStateIfUpwards(requiredState));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {})).join();

        return futures.stream()
            .map(CompletableFuture::join)
            .reduce(CheckResourcesResult.AVAILABLE, CheckResourcesResult::worstCase);
    }

    private boolean transitionEnsembleStateUpwards(Optional<NodeGroup.State> maximumState,
        TestRun.State testRunState, String stage)
    {
        CheckResourcesResult transitionResult = CheckResourcesResult.FAILED;

        setTestRunState(testRunState);

        try
        {
            transitionResult = transitionEnsembleStateUpwards(maximumState);
            failTestIfCheckResourcesDidNotSucceed(transitionResult, stage);
        }
        catch (Exception e)
        {
            failTest("Error " + stage + " ensemble for test run", e);
        }

        return transitionResult.wasSuccessful();
    }

    private void setTestRunState(TestRun.State state)
    {
        testRunStatusUpdater.setCurrentState(state);
    }

    private void failTestTemporarily(String message)
    {
        logger.info(message);
        testRunStatusUpdater.markFailedWithReason(TestRun.State.WAITING_FOR_RESOURCES);
    }

    @VisibleForTesting
    void failTest(String message)
    {
        logger.error(message);
        testRunStatusUpdater.markFailedWithReason(TestRun.State.FAILED);
    }

    @VisibleForTesting
    void failTest(String message, Throwable t)
    {
        logger.error(message, t);
        testRunStatusUpdater.markFailedWithReason(TestRun.State.FAILED);
    }

    @VisibleForTesting
    boolean setup()
    {
        // Check before, but not after, as checking after could cause
        // cleanup required by a successful call of f to be skipped.
        return !testRunStatusUpdater.hasBeenAborted() && doSetup();
    }

    private boolean doSetup()
    {
        try
        {
            logger.info("Setting up ensemble before beginning jepsen test");

            if (!ensemble.createAllLocalFiles())
            {
                return false;
            }

            // Check states and kick off any post-check-state actions.
            final List<CompletableFuture<Boolean>> postCheckStateActions = ensemble
                .getUniqueNodeGroupInstances()
                .stream()
                .map(NodeGroup::checkState)
                .map(CompletableFuture::join)
                .map(postCheckStateAction -> CompletableFuture.supplyAsync(postCheckStateAction::getAsBoolean))
                .collect(Collectors.toList());

            return transitionEnsembleStateUpwards(Optional.of(NodeGroup.State.RESERVED),
                TestRun.State.RESERVING_RESOURCES, "reserving") &&
                transitionEnsembleStateUpwards(Optional.of(NodeGroup.State.CREATED),
                    TestRun.State.ACQUIRING_RESOURCES, "acquiring") &&
                transitionEnsembleStateUpwards(Optional.of(NodeGroup.State.STARTED_SERVICES_CONFIGURED),
                    TestRun.State.SETTING_UP, "setting up") &&
                Utils.waitForAll(postCheckStateActions, logger, "post check-state actions") &&
                transitionEnsembleStateUpwards(Optional.empty(),
                    TestRun.State.SETTING_UP, "setting up");
        }
        finally
        {
            // Summarize ensemble info to aid post-hoc debugging
            String downloadPath = testRunArtifactPath.toAbsolutePath().toString();
            Path ensembleSummary = Paths.get(downloadPath, "ensemble-summary.json");
            HashMap<String, Object> ensembleInfo = new HashMap<>();
            DebugInfoProvidingComponent.InfoConsumer infoConsumer = ensembleInfo::put;

            infoConsumer.accept("fallout_version", FalloutVersion.getVersion());
            ensemble.summarizeInfo(infoConsumer);
            try
            {
                ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
                writer.writeValue(ensembleSummary.toFile(), ensembleInfo);
            }
            catch (IOException e)
            {
                logger.warn("Error writing summaryInfo to file", e);
            }

        }
    }

    @VisibleForTesting
    void runWorkload()
    {
        if (!testRunStatusUpdater.hasBeenAborted())
        {
            try
            {
                result = JepsenWorkload.run(logger, ensemble, workload, testRunStatusUpdater);
            }
            catch (Exception e)
            {
                failTest("Unexpected exception", e);
            }
        }
    }

    private void writeJepsenHistory(Collection<Operation> history, Path jepsenHistoryPath)
    {
        ObjectWriter writer = new ObjectMapper().writer(new MinimalPrettyPrinter()
        {
            @Override
            public void writeStartArray(JsonGenerator jg) throws IOException, JsonGenerationException
            {
                super.writeStartArray(jg);
                jg.writeRaw('\n');
            }

            @Override
            public void writeArrayValueSeparator(JsonGenerator jg) throws IOException, JsonGenerationException
            {
                super.writeArrayValueSeparator(jg);
                jg.writeRaw('\n');
            }

            @Override
            public void writeEndArray(JsonGenerator jg, int nrOfValues) throws IOException, JsonGenerationException
            {
                jg.writeRaw('\n');
                super.writeEndArray(jg, nrOfValues);
            }
        });

        try
        {
            Files.createDirectories(jepsenHistoryPath.getParent());
            Files.write(jepsenHistoryPath,
                writer.writeValueAsString(history).getBytes(StandardCharsets.UTF_8));
        }
        catch (IOException e)
        {
            failTest("Error writing Jepsen history to file", e);
        }
    }

    @VisibleForTesting
    void tearDown()
    {
        try
        {
            logger.info("Tearing down after test run");

            setTestRunState(TestRun.State.TEARING_DOWN);

            Path controllerGroupArtifactPath = ensemble.getControllerGroup().getLocalArtifactPath();

            result.ifPresent(result_ -> writeJepsenHistory(result_.history(),
                controllerGroupArtifactPath.resolve("jepsen-history.json")));

            // Tearing down the ensemble is unrelated to the final test state.
            // For example, a module could fail the test but all node groups are STARTED_SERVICES_RUNNING.
            // Hence, tear down considerations need to be based on the node group states.

            // At this point, the node groups can be in one of 3 situations:
            // 1) The node groups are all a run level greater than FAILED / DESTROYED.
            // 2) One or more node groups are FAILED.
            // 3) One or more node groups are MARKED FOR REUSE.
            //
            // Cases (1) and (2) should be handled in the same way: downward transitions only!
            // Case (3) should just skip all transitions and attempt to prepare & collect artifacts.

            //prepare artifacts. maybe stop services. collect artifacts. download
            CompletableFuture<Boolean> download = ensemble.prepareArtifacts()
                .thenComposeAsync(prepared -> {
                    if (!prepared)
                    {
                        failTest("Problem preparing artifacts - attempting to collect artifacts anyway");
                    }
                    return transitionEnsembleForTearDown(Optional.of(NodeGroup.State.STARTED_SERVICES_CONFIGURED));
                })
                .thenComposeAsync(stopped -> {
                    if (!stopped)
                    {
                        failTest("Problem stopping services - attempting to collect artifacts anyway");
                    }
                    return ensemble.collectArtifacts();
                })
                .thenComposeAsync(collected -> {
                    if (!collected)
                    {
                        failTest("Error collecting artifacts, downloading what we were able to retrieve");
                    }
                    return ensemble.downloadArtifacts();
                })
                .exceptionally(t -> {
                    failTest("Error downloading artifacts", t);
                    return false;
                });

            if (download.join())
            {
                logger.info("Downloaded artifacts to {}", testRunArtifactPath);
            }

            if (ensembleOwner && !transitionEnsembleForTearDown(Optional.empty()).join())
            {
                failTest(String.format("Problem destroying node group in test instance %s, forcing destroy", this));
                if (!ensemble.forceDestroy().join())
                {
                    failTest("Forcing destroy failed, check resources");
                }
            }

            if (download.join())
            {
                setTestRunState(TestRun.State.CHECKING_ARTIFACTS);
                logger.info("Running artifact_checkers");
                boolean validationFailed = runAndCheckIfArtifactCheckerValidationFailed(testRunArtifactPath);
                if (result.isPresent() && validationFailed)
                {
                    failTest("artifact_checkers validation failed. Setting test result to valid=false");
                    result.get().setArtifactCheckersValid(false);
                }
            }
            else
            {
                failTest("Artifacts were not downloaded! Artifact checker results may be unreliable.");
                runAndCheckIfArtifactCheckerValidationFailed(testRunArtifactPath);
            }

        }
        catch (Throwable e)
        {
            failTest("Error in teardown, forcing destroy ", e);
            try
            {
                if (!ensemble.forceDestroy().join())
                {
                    failTest("Forcing destroy failed, check resources");
                }
            }
            catch (Throwable e2)
            {
                failTest("Forcing destroy failed, check resources ", e2);
            }
        }
    }

    private CompletableFuture<Boolean> transitionEnsembleForTearDown(Optional<NodeGroup.State> endState)
    {
        List<CompletableFuture<Boolean>> transitions = ensemble.getUniqueNodeGroupInstances().stream()
            .map(ng -> ng.transitionStateIfDownwards(getStateForTearDownTransition(ng, endState))
                .thenApplyAsync(CheckResourcesResult::wasSuccessful))
            .collect(Collectors.toList());

        return Utils.waitForAllAsync(transitions);
    }

    /**
     * If finalRunLevel is empty, then no transition needed because the current node group state should be preserved.
     * If endState is empty, then use finalRunLevel
     * Otherwise, use the maximum of endState and finalRunLevel
     */
    private NodeGroup.State getStateForTearDownTransition(NodeGroup nodeGroup, Optional<NodeGroup.State> endState)
    {
        return nodeGroup.getFinalRunLevel()
            .map(finalRunLevel -> endState.filter(endState_ -> finalRunLevel.ordinal() <= endState_.ordinal())
                .orElse(finalRunLevel))
            .orElse(nodeGroup.getState());
    }

    private boolean runAndCheckIfArtifactCheckerValidationFailed(Path rootArtifactLocation)
    {
        List<Boolean> results = new ArrayList<>();
        for (Map.Entry<String, ArtifactChecker> entry : workload.getArtifactCheckers().entrySet())
        {
            ArtifactChecker checker = entry.getValue();
            boolean checkResult = checker.validate(ensemble, rootArtifactLocation);
            if (checkResult)
            {
                logger.info("ArtifactChecker '{}' has passed the check.", checker.getInstanceName());
            }
            else
            {
                logger.error("ArtifactChecker '{}' has failed the check.", checker.getInstanceName());
            }
            results.add(checkResult);
        }

        return results.stream().anyMatch(res -> !res);
    }

    @VisibleForTesting
    public void close()
    {
        testRunStatusUpdater.markInactive(result);
        ensemble.close();
    }

    private static void doWithoutThrowing(Runnable operation, String operationName,
        ExceptionHandler exceptionHandler)
    {
        doWithoutThrowing(() -> { operation.run(); return true; }, operationName, exceptionHandler, s -> {});
    }

    private static boolean doWithoutThrowing(Supplier<Boolean> operation, String operationName,
        ExceptionHandler exceptionHandler, Consumer<String> failureHandler)
    {
        return doWithoutThrowing(() -> CheckResourcesResult.fromWasSuccessful(operation.get()),
            operationName, exceptionHandler, failureHandler, failureHandler);
    }

    private static boolean doWithoutThrowing(Supplier<CheckResourcesResult> operation, String operationName,
        ExceptionHandler exceptionHandler, Consumer<String> failureHandler,
        Consumer<String> temporaryFailureHandler)
    {
        CheckResourcesResult result = CheckResourcesResult.FAILED;
        try
        {
            result = operation.get();
            switch (result)
            {
                case FAILED:
                    failureHandler.accept(operationName + " failed");
                    break;
                case UNAVAILABLE:
                    temporaryFailureHandler.accept(operationName + " failed temporarily");
                    break;
            }
            return result.wasSuccessful();
        }
        catch (Throwable e)
        {
            exceptionHandler.accept(operationName + " threw an exception", e);
            return false;
        }
    }

    public Set<ResourceRequirement> getResourceRequirements()
    {
        return ensemble.getResourceRequirements();
    }

    /** Run the ActiveTestRun.  Does not allow any exceptions to escape: they will be logged to the testrun log; if
     *  that is not possible, they will be logged to lastResortExceptionHandler. */
    public void run(ExceptionHandler lastResortExceptionHandler)
    {
        run(this, lastResortExceptionHandler);
    }

    @VisibleForTesting
    static void run(ActiveTestRun activeTestRun, ExceptionHandler lastResortExceptionHandler)
    {
        ExceptionHandler logExceptionAndFailTestRun = (msg, e) -> {
            doWithoutThrowing(() -> activeTestRun.failTest(msg, e),
                "Fail", lastResortExceptionHandler);
        };

        if (doWithoutThrowing(activeTestRun::checkResources, "Resource check",
            logExceptionAndFailTestRun, activeTestRun::failTest, activeTestRun::failTestTemporarily))
        {
            if (doWithoutThrowing(activeTestRun::setup, "Setup",
                logExceptionAndFailTestRun, activeTestRun::failTest))
            {
                doWithoutThrowing(activeTestRun::runWorkload, "Run Workload", logExceptionAndFailTestRun);
            }
            doWithoutThrowing(activeTestRun::tearDown, "Tear down", logExceptionAndFailTestRun);
        }

        // We shouldn't use an ExceptionHandler that could use the ActiveTestRun itself, as we're closing and not
        // all resources may be available.
        doWithoutThrowing(activeTestRun::close, "Close", lastResortExceptionHandler);
    }
}
