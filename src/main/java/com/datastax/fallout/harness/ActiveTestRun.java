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
package com.datastax.fallout.harness;

import java.io.IOException;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.datastax.fallout.ops.TestRunScratchSpaceFactory.TestRunScratchSpace;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.core.TestRun;

/**
 * A class with all the information necessary to setup an {@link Ensemble} and run a {@link Workload} in the
 * Jepsen test harness.
 */
public class ActiveTestRun implements AutoCloseable
{
    private final Ensemble ensemble;
    private final Workload workload;
    private final TestRunAbortedStatusUpdater testRunStatusUpdater;
    private final Path testRunArtifactPath;
    private final Logger logger;
    private final Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker;
    private final Function<Ensemble, Boolean> postSetupHook;
    private final TestRunScratchSpace testRunScratchSpace;

    private Optional<TestResult> result = Optional.empty();

    ActiveTestRun(Ensemble ensemble, Workload workload,
        TestRunAbortedStatusUpdater testRunStatusUpdater,
        Path testRunArtifactPath,
        Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker,
        Function<Ensemble, Boolean> postSetupHook,
        TestRunScratchSpace testRunScratchSpace)
    {
        this.ensemble = ensemble;
        this.workload = workload;
        this.testRunStatusUpdater = testRunStatusUpdater;
        this.testRunArtifactPath = testRunArtifactPath;
        this.logger = ensemble.getControllerGroup().logger();
        this.resourceChecker = resourceChecker;
        this.postSetupHook = postSetupHook;
        this.testRunScratchSpace = testRunScratchSpace;

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
        for (Checker checker : workload.getCheckers())
        {
            boolean currComponentHasValidPrefixes = checker.validatePrefixes(logger);
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
            List<PropertySpec<?>> combinedSpecs = ImmutableList.<PropertySpec<?>>builder()
                .addAll(ng.getProvisioner().getPropertySpecs())
                .addAll(ng.getConfigurationManager().getPropertySpecs())
                .build();
            ng.getProperties().validateFull(combinedSpecs);
        }

        Stream
            .concat(
                workload.getPhases().stream()
                    .flatMap(phase -> phase.getAllModulesRecursively().values().stream()),
                Stream.concat(
                    workload.getCheckers().stream(),
                    workload.getArtifactCheckers().stream()))
            .forEach(component -> component.getProperties().validateFull(component.getPropertySpecs()));

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
    public Workload getWorkload()
    {
        return workload;
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
                break;
            case FAILED:
                failTest("Error " + stage + " ensemble for testrun, check logs");
                break;
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

    private CheckResourcesResult transitionEnsembleStateUpwards(Optional<NodeGroup.State> maximumState,
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

        return transitionResult;
    }

    private void setTestRunState(TestRun.State state)
    {
        testRunStatusUpdater.setCurrentState(state);
    }

    @VisibleForTesting
    void failTestTemporarily(String message)
    {
        logger.info(message);
        testRunStatusUpdater.markFailedWithReason(TestRun.State.WAITING_FOR_RESOURCES);
    }

    private void failTest(String message)
    {
        failTest(message, null);
    }

    @VisibleForTesting
    void failTest(String message, Throwable t)
    {
        if (t != null)
        {
            logger.error(message, t);
        }
        else
        {
            logger.error(message);
        }
        testRunStatusUpdater.markFailedWithReason(TestRun.State.FAILED);
    }

    @VisibleForTesting
    CheckResourcesResult setup()
    {
        // Check before, but not after, as checking after could cause
        // cleanup required by a successful call of f to be skipped.
        return CheckResourcesResult.fromWasSuccessful(!testRunStatusUpdater.hasBeenAborted())
            .ifSuccessful(this::doSetup);
    }

    private CheckResourcesResult doSetup()
    {
        try
        {
            logger.info("Setting up ensemble before beginning jepsen test");

            // Check states and kick off any post-check-state actions.
            final List<CompletableFuture<Boolean>> postCheckStateActions = ensemble
                .getUniqueNodeGroupInstances()
                .stream()
                .map(NodeGroup::checkState)
                .map(CompletableFuture::join)
                .map(postCheckStateAction -> CompletableFuture.supplyAsync(postCheckStateAction::getAsBoolean))
                .collect(Collectors.toList());

            return transitionEnsembleStateUpwards(Optional.of(NodeGroup.State.RESERVED),
                TestRun.State.RESERVING_RESOURCES, "reserving")
                    .ifSuccessful(
                        () -> transitionEnsembleStateUpwards(Optional.of(NodeGroup.State.CREATED),
                            TestRun.State.ACQUIRING_RESOURCES, "acquiring"))
                    .ifSuccessful(
                        () -> transitionEnsembleStateUpwards(Optional.of(NodeGroup.State.STARTED_SERVICES_CONFIGURED),
                            TestRun.State.SETTING_UP, "setting up"))
                    .ifSuccessful(
                        () -> CheckResourcesResult.fromWasSuccessful(
                            postSetupHook.apply(ensemble)))
                    .ifSuccessful(
                        () -> CheckResourcesResult.fromWasSuccessful(
                            Utils.waitForAll(postCheckStateActions, logger, "post check-state actions")))
                    .ifSuccessful(
                        () -> transitionEnsembleStateUpwards(Optional.empty(),
                            TestRun.State.SETTING_UP, "setting up"));
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

    /** Run the workload and returns whether it was run; note that a false result does <em>not</em> indicate
     *  failure, it just means the workload wasn't run (because the testrun was aborted for example) */
    @VisibleForTesting
    boolean runWorkload()
    {
        if (!testRunStatusUpdater.hasBeenAborted())
        {
            // result.isPresent() is unfortunately not necessarily the same as the workload being run, because
            // the jepsen clojure implementation could run the workload but return empty results.
            result = JepsenWorkload.run(logger, ensemble, workload, testRunStatusUpdater);
            return true;
        }
        return false;
    }

    private void writeJepsenHistory(Collection<Operation> history, Path jepsenHistoryPath)
    {
        ObjectWriter writer = new ObjectMapper().writer(new MinimalPrettyPrinter() {
            @Override
            public void writeStartArray(JsonGenerator jg) throws IOException
            {
                super.writeStartArray(jg);
                jg.writeRaw('\n');
            }

            @Override
            public void writeArrayValueSeparator(JsonGenerator jg) throws IOException
            {
                super.writeArrayValueSeparator(jg);
                jg.writeRaw('\n');
            }

            @Override
            public void writeEndArray(JsonGenerator jg, int nrOfValues) throws IOException
            {
                jg.writeRaw('\n');
                super.writeEndArray(jg, nrOfValues);
            }
        });

        try
        {
            Files.createDirectories(jepsenHistoryPath.getParent());
            Files.writeString(jepsenHistoryPath, writer.writeValueAsString(history));
        }
        catch (IOException e)
        {
            failTest("Error writing Jepsen history to file", e);
        }
    }

    @VisibleForTesting
    public void startTearDown()
    {
        setTestRunState(TestRun.State.TEARING_DOWN);
    }

    @VisibleForTesting
    public void downloadArtifacts()
    {
        logger.info("Downloading artifacts");

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
        else
        {
            failTest("Artifacts were not downloaded! Artifact checker results may be unreliable.");
        }
    }

    @VisibleForTesting
    public void tearDownEnsemble()
    {
        logger.info("Tearing down ensemble");

        boolean tornDown = false;
        Throwable e = null;

        try
        {
            tornDown = transitionEnsembleForTearDown(Optional.empty()).join();
        }
        catch (Throwable e_)
        {
            e = e_;
        }

        if (tornDown)
        {
            return;
        }

        failTest("Failed to teardown ensemble: forcing destroy for non-reused node groups", e);

        try
        {
            tornDown = ensemble.forceDestroy().join();
        }
        catch (Throwable e_)
        {
            e = e_;
        }

        if (tornDown)
        {
            return;
        }

        failTest("Forcing destroy failed: please make sure to clean up allocated resources manually!", e);
    }

    @VisibleForTesting
    public void checkArtifacts()
    {
        setTestRunState(TestRun.State.CHECKING_ARTIFACTS);
        logger.info("Running artifact_checkers");

        if (runAndCheckIfArtifactCheckerValidationFailed(testRunArtifactPath))
        {
            failTest("artifact_checkers validation failed. Setting test result to valid=false");

            // result may or may not have been returned by jepsen; if it hasn't, then the testrun will already
            // have been failed by JepsenWorkload.run
            result.ifPresent(result_ -> result_.setArtifactCheckersValid(false));
        }
    }

    private CompletableFuture<Boolean> transitionEnsembleForTearDown(Optional<NodeGroup.State> endState)
    {
        List<CompletableFuture<Boolean>> transitions = ensemble.getUniqueNodeGroupInstances().stream()
            .flatMap(ng -> getStateForTearDownTransition(ng, endState)
                .map(tearDownState -> ng.transitionStateIfDownwards(tearDownState)
                    .thenApplyAsync(CheckResourcesResult::wasSuccessful))
                .stream())
            .collect(Collectors.toList());

        return Utils.waitForAllAsync(transitions);
    }

    /** Calculate the {@link NodeGroup.State} that should be used when the code requires
     * a transition during teardown to:
     *
     * <ul>
     *     <li>a specific state (<code>endState</code> is not empty)
     *     <li>or the state specified by {@link NodeGroup#getFinalRunLevel()} (<code>endState is empty</code>)
     * </ul>
     *
     * <p>The intent is to return a state that honours {@link NodeGroup#getFinalRunLevel()}:
     *
     * <ul>
     *     <li>if {@link NodeGroup#getFinalRunLevel()} is empty, then no transition should occur because we need to preserve the current state, and we return empty;
     *     <li>otherwise, if <code>endState</code> is set, then we should we use the higher state i.e. we should not go below {@link NodeGroup#getFinalRunLevel()};
     *     <li>otherwise, we should use {@link NodeGroup#getFinalRunLevel()} (which defaults to {@link NodeGroup.State#DESTROYED}).
     * </ul>
     */
    private Optional<NodeGroup.State> getStateForTearDownTransition(NodeGroup nodeGroup,
        Optional<NodeGroup.State> endState)
    {
        return nodeGroup.getFinalRunLevel()
            .map(finalRunLevel -> endState.filter(endState_ -> finalRunLevel.ordinal() <= endState_.ordinal())
                .orElse(finalRunLevel));
    }

    private boolean runAndCheckIfArtifactCheckerValidationFailed(Path rootArtifactLocation)
    {
        List<Boolean> results = new ArrayList<>();
        for (ArtifactChecker checker : workload.getArtifactCheckers())
        {
            boolean checkResult = checker.checkArtifacts(ensemble, rootArtifactLocation);
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
        testRunScratchSpace.close();
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

    /** Encapsulate exception and result handling for each stage of an {@link ActiveTestRun} */
    private static class WithoutThrowing
    {
        private final ActiveTestRun activeTestRun;
        private final ExceptionHandler lastResortExceptionHandler;

        WithoutThrowing(ActiveTestRun activeTestRun, ExceptionHandler lastResortExceptionHandler)
        {
            this.activeTestRun = activeTestRun;
            this.lastResortExceptionHandler = lastResortExceptionHandler;
        }

        private void logExceptionAndFailTestRun(String msg, Throwable e)
        {
            handle("Fail", () -> activeTestRun.failTest(msg, e), lastResortExceptionHandler);
        }

        /** Return the result of operation, handling any exceptions using exceptionHandler,
         * and treating {@link CheckResourcesResult#FAILED} as a test failure, and
         * {@link CheckResourcesResult#UNAVAILABLE} as a temporary test failure */
        private CheckResourcesResult handleFailureAndUnavailable(String operationName,
            Supplier<CheckResourcesResult> operation,
            ExceptionHandler exceptionHandler)
        {
            try
            {
                final var result = operation.get();
                switch (result)
                {
                    case FAILED:
                        activeTestRun.failTest(operationName + " failed");
                        break;
                    case UNAVAILABLE:
                        activeTestRun.failTestTemporarily(operationName + " failed temporarily");
                        break;
                    default:
                        break;
                }
                return result;
            }
            catch (Throwable e)
            {
                exceptionHandler.accept(operationName + " threw an exception", e);
                return CheckResourcesResult.FAILED;
            }
        }

        /** Return the result of operation, handling any exceptions, and treating {@link CheckResourcesResult#FAILED}
         *  as a test failure, and {@link CheckResourcesResult#UNAVAILABLE} as a temporary test failure */
        CheckResourcesResult handleFailureAndUnavailable(String operationName, Supplier<CheckResourcesResult> operation)
        {
            return handleFailureAndUnavailable(operationName, operation, this::logExceptionAndFailTestRun);
        }

        /** Return the result of operation, handling any exceptions, but not allowing the result
         *  to affect test success/failure */
        boolean handle(String operationName, Supplier<Boolean> operation)
        {
            final var result = new AtomicBoolean(false);
            handleFailureAndUnavailable(operationName, () -> {
                result.set(operation.get());
                return CheckResourcesResult.AVAILABLE;
            }, this::logExceptionAndFailTestRun);
            return result.get();
        }

        /** Run operation, handling any exceptions */
        void handle(String operationName, Runnable operation)
        {
            handle(operationName, () -> {
                operation.run();
                return true;
            });
        }

        /** Run operation, handling any exceptions using exceptionHandler */
        void handle(String operationName, Runnable operation, ExceptionHandler exceptionHandler)
        {
            handleFailureAndUnavailable(operationName, () -> {
                operation.run();
                return CheckResourcesResult.AVAILABLE;
            }, exceptionHandler);
        }

        void run()
        {
            if (handleFailureAndUnavailable("Resource check", activeTestRun::checkResources).wasSuccessful())
            {
                final var setupResult = handleFailureAndUnavailable("Setup", activeTestRun::setup);

                final var workloadWasRun =
                    setupResult.wasSuccessful() &&
                        handle("Run Workload", activeTestRun::runWorkload);

                handle("Start test run tear down", activeTestRun::startTearDown);

                // Only skip downloading artifacts if some resources weren't available at
                // setup time; in the case of FAILED setup, we want artifacts for diagnosis.
                if (setupResult != CheckResourcesResult.UNAVAILABLE)
                {
                    handle("Download artifacts", activeTestRun::downloadArtifacts);
                }

                handle("Tear down ensemble", activeTestRun::tearDownEnsemble);

                if (workloadWasRun)
                {
                    handle("Check artifacts", activeTestRun::checkArtifacts);
                }
            }

            // We shouldn't use an ExceptionHandler that could use the ActiveTestRun itself, as we're closing and not
            // all resources may be available.
            handle("Close", activeTestRun::close, lastResortExceptionHandler);
        }
    }

    @VisibleForTesting
    static void run(ActiveTestRun activeTestRun, ExceptionHandler lastResortExceptionHandler)
    {
        new WithoutThrowing(activeTestRun, lastResortExceptionHandler).run();
    }
}
