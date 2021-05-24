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
package com.datastax.fallout.runner;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.fallout.FalloutVersion;
import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.ActiveTestRunBuilder;
import com.datastax.fallout.harness.InMemoryTestRunStateStorage;
import com.datastax.fallout.harness.TestRunAbortedStatusUpdater;
import com.datastax.fallout.harness.TestRunLinkUpdater;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.JobLoggers;
import com.datastax.fallout.ops.NullJobLoggers;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.Java8LocalCommandExecutor;
import com.datastax.fallout.ops.commands.LocalCommandExecutor;
import com.datastax.fallout.ops.commands.RejectableCommandExecutor;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ComponentFactory;

/** Responsible for turning a {@link TestRun} into a runnable {@link ActiveTestRun} */
public class ActiveTestRunFactory
{
    private ComponentFactory componentFactory = null;
    private Supplier<CommandExecutor> commandExecutorFactory = LocalCommandExecutor::new;
    private final FalloutConfiguration configuration;
    private final TestRunScratchSpaceFactory testRunScratchSpaceFactory;

    private Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker = ensemble -> List.of();
    private Function<Ensemble, Boolean> postSetupHook = ensemble -> true;

    public ActiveTestRunFactory(FalloutConfiguration configuration)
    {
        this.configuration = configuration;
        if (configuration.forceJava8ForLocalCommands())
        {
            commandExecutorFactory = Java8LocalCommandExecutor::new;
        }
        testRunScratchSpaceFactory = new TestRunScratchSpaceFactory(
            configuration.getRunDir().resolve("tmp"));
    }

    @VisibleForTesting
    public ActiveTestRunFactory withComponentFactory(ComponentFactory componentFactory)
    {
        this.componentFactory = componentFactory;
        return this;
    }

    @VisibleForTesting
    public ActiveTestRunFactory withCommandExecutorFactory(Supplier<CommandExecutor> commandExecutorFactory)
    {
        this.commandExecutorFactory = commandExecutorFactory;
        return this;
    }

    @VisibleForTesting
    public ActiveTestRunFactory
        withResourceChecker(Function<Ensemble, List<CompletableFuture<Boolean>>> resourceChecker)
    {
        this.resourceChecker = resourceChecker;
        return this;
    }

    public ActiveTestRunFactory withPostSetupHook(Function<Ensemble, Boolean> postSetupHook)
    {
        this.postSetupHook = postSetupHook;
        return this;
    }

    private ActiveTestRunBuilder createBuilder(TestRun testRun, UserCredentials userCredentials)
    {
        String expandedDefinition = testRun.getExpandedDefinition();

        return ActiveTestRunBuilder.create()
            .withFalloutConfiguration(configuration)
            .withTestRunIdentifier(testRun.getTestRunIdentifier())
            .withTestDefinitionFromYaml(expandedDefinition)
            .withUserCredentials(userCredentials)
            .withTestRunArtifactPath(Artifacts.buildTestRunArtifactPath(configuration, testRun))
            .withResourceChecker(resourceChecker);
    }

    public Optional<ActiveTestRun> create(TestRun testRun, UserCredentials userCredentials,
        JobLoggers loggers, TestRunAbortedStatusUpdater testRunStatus, TestRunLinkUpdater testRunLinkUpdater)
    {
        final var testRunId = testRun.getTestRunId();

        testRunStatus.setCurrentState(TestRun.State.PREPARING_RUN);

        loggers.getShared().info("Test run started for {} {}", testRun.getTestName(), testRunId);
        loggers.getShared().info("Current Fallout version is {}.", FalloutVersion.getVersion());

        Optional<ActiveTestRun> activeTestRun = Optional.empty();
        try
        {
            activeTestRun = Optional.of(
                createBuilder(testRun, userCredentials)
                    .withComponentFactory(componentFactory)
                    .withCommandExecutor(new RejectableCommandExecutor(testRunStatus, commandExecutorFactory.get()))
                    .withTestRunStatusUpdater(testRunStatus)
                    .withLoggers(loggers)
                    .withTestRunScratchSpace(testRunScratchSpaceFactory.create(testRun.getTestRunIdentifier()))
                    .withTestRun(testRun)
                    .withResourceChecker(resourceChecker)
                    .withPostSetupHook(postSetupHook)
                    .withTestRunLinkUpdater(testRunLinkUpdater)
                    .build());
        }
        catch (Exception e)
        {
            loggers.getShared().error("Could not create ActiveTestRun", e);
            testRunStatus.markFailedWithReason(TestRun.State.FAILED);
            testRunStatus.markInactive(Optional.empty());
        }

        return activeTestRun;
    }

    /** Returns the expanded test definition if the test is valid, or throws an Exception */
    public void validateTestDefinition(Test test, UserCredentialsFactory userCredentialsFactory)
    {
        validateTestDefinition(test, userCredentialsFactory, Map.of());
    }

    /** Returns the expanded test definition if the test is valid, or throws an Exception */
    public String validateTestDefinition(Test test, UserCredentialsFactory userCredentialsFactory,
        Map<String, Object> templateParams)
    {
        TestRun testRun = test.createTestRun(templateParams);

        // Creating the ActiveTestRun will also validate it
        try (var ignored = createTemporaryActiveTestRun(userCredentialsFactory, testRun))
        {
            return testRun.getExpandedDefinition();
        }
    }

    public Set<ResourceRequirement> getResourceRequirements(TestRun testRun,
        UserCredentialsFactory userCredentialsFactory)
    {
        try (var activeTestRun = createTemporaryActiveTestRun(userCredentialsFactory, testRun))
        {
            return activeTestRun.getResourceRequirements();
        }
    }

    /** Create the bare minimum of an {@link ActiveTestRun} required for {@link
     *  #validateTestDefinition} and {@link #getResourceRequirements} */
    private ActiveTestRun createTemporaryActiveTestRun(UserCredentialsFactory userCredentialsFactory, TestRun testRun)
    {
        return createBuilder(testRun, userCredentialsFactory.apply(testRun))
            .withTestRunStatusUpdater(
                new TestRunAbortedStatusUpdater(new InMemoryTestRunStateStorage(testRun.getState())))
            .withLoggers(new NullJobLoggers())
            // TODO: It's possible that the use-cases for the created ActiveTestRun do not require a scratch space
            .withTestRunScratchSpace(testRunScratchSpaceFactory.create(testRun.getTestRunIdentifier()))
            .build();
    }
}
