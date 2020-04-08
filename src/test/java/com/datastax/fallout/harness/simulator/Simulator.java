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
package com.datastax.fallout.harness.simulator;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.harness.TestRunnerTestHelpers;
import com.datastax.fallout.harness.impl.FakeModule;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;
import com.datastax.fallout.runner.ActiveTestRunFactory;
import com.datastax.fallout.runner.JobLoggersFactory;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.runner.ResourceReservationLocks;
import com.datastax.fallout.runner.ThreadedRunnableExecutorFactory;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.runner.queue.InMemoryPendingQueue;
import com.datastax.fallout.runner.queue.TestRunQueue;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ScopedLogger;

import static com.datastax.fallout.harness.TestRunnerTestHelpers.makeTest;
import static com.datastax.fallout.harness.simulator.TestRunPlan.shortTestRunId;

/**
 * Simulator creates a TestRunner, feeds it TestRuns and the results of calls to a Module and a Provisioner, and
 * monitors what it does to ensure correct behaviour.  The state of the TestRunner is managed by
 * {@link TestRunnerState}, which is also responsible for asserting that the state is as expected.
 *
 * The heart of Simulator is {@link #simulate(QueuingTestRunner)}, which handles stepping through the simulation according
 * to the instructions in the {@link #pendingTestRunPlans}.
 */
public class Simulator extends TestHelpers.FalloutTest
{
    static final ScopedLogger logger = ScopedLogger.getLogger(Simulator.class);
    static final Duration SYNCHRONIZATION_TIMEOUT = Duration.ofSeconds(5);

    private final Queue<List<TestRunPlan>> pendingTestRunPlans;
    private final Map<Long, TestRunPlan> runningTestRunPlans = new HashMap<>();

    private final User user = getTestUser();
    private final Test test;
    private final Fakes.TestRunFactory testRunFactory;
    private final TestRunnerState testRunnerState = new TestRunnerState();
    private final TestRunQueue.Blocker blocker = new TestRunQueue.Blocker();
    private final int simulationId;

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    static void debug(String message, Object... args)
    {
        logger.info(String.format("### " + message, args));
    }

    static class SimulatedModule extends FakeModule
    {
        static final PropertySpec<UUID> testRunIdSpec = PropertySpecBuilder.create(PREFIX)
            .name("test.run.id")
            .parser(input -> UUID.fromString((String) input))
            .required()
            .build();

        final TestRunnerState testRunnerState;

        SimulatedModule(TestRunnerState testRunnerState)
        {
            this.testRunnerState = testRunnerState;
        }

        @Override
        public List<PropertySpec> getModulePropertySpecs()
        {
            return ImmutableList.of(testRunIdSpec);
        }

        @Override
        public void run(Ensemble ensemble, PropertyGroup properties)
        {
            emit(Operation.Type.invoke);
            testRunnerState.testRunner_addTestRunToWaitingTestRunsAndWaitForRelease(
                shortTestRunId(testRunIdSpec.value(properties)));
            emit(Operation.Type.ok);
        }
    }

    static class SimulatedProvisioner extends FakeProvisioner
    {
        static final PropertySpec<UUID> testRunIdSpec = PropertySpecBuilder.create(PREFIX)
            .name("test.run.id")
            .parser(input -> UUID.fromString((String) input))
            .required()
            .build();

        final TestRunnerState testRunnerState;

        SimulatedProvisioner(TestRunnerState testRunnerState)
        {
            this.testRunnerState = testRunnerState;
        }

        @Override
        public List<PropertySpec> getPropertySpecs()
        {
            return ImmutableList.of(testRunIdSpec);
        }

        public TestRunnerState getTestRunnerState()
        {
            return testRunnerState;
        }

        public static List<CompletableFuture<Boolean>> getMockResChecks(Ensemble ensemble)
        {
            return ensemble.getUniqueNodeGroupInstances().stream()
                .filter(nodeGroup -> nodeGroup.getProvisioner() instanceof SimulatedProvisioner)
                .map(nodeGroup -> CompletableFuture.completedFuture(
                    ((SimulatedProvisioner) nodeGroup.getProvisioner()).getTestRunnerState()
                        .testRunner_checkResources(shortTestRunId(testRunIdSpec.value(nodeGroup)))))
                .collect(Collectors.toList());
        }
    }

    Simulator(int simulationId, List<List<TestRunPlan>> pendingTestRunPlans)
    {
        debug("===");
        debug("Simulating %s", pendingTestRunPlans);

        testRunFactory = new Fakes.TestRunFactory(new UUID(simulationId, 0));

        this.pendingTestRunPlans = new ArrayDeque<>(pendingTestRunPlans);
        this.simulationId = simulationId;
        user.setEmail("simulator@example.com");
        test = makeTest(user.getEmail(), "fakes.yaml");
    }

    /** Run _simulate as a JUnit4 test method: this allows us to use all the JUnit-based annotations and helpers
     *  that have been built up for use with other tests */
    void simulate()
    {
        Exceptions.runUnchecked(() -> {
            // Inspired by https://stackoverflow.com/a/13606735/322152
            final BlockJUnit4ClassRunner runner = new BlockJUnit4ClassRunner(Simulator.class)
            {
                /** Instead of a new instance of Simulator, return the enclosing instance */
                @Override
                protected Object createTest() throws Exception
                {
                    return Simulator.this;
                }

                /** We have only one test method, _simulate */
                @Override
                protected List<FrameworkMethod> computeTestMethods()
                {
                    return Exceptions.getUnchecked(
                        () -> ImmutableList.of(new FrameworkMethod(Simulator.class.getMethod("_simulate"))));
                }

                /** Return an appropriate name: we use the simulationId */
                @Override
                protected String testName(FrameworkMethod method)
                {
                    return String.format("simulate[%s]", simulationId);
                }

                /** Stop JUnit from moaning about the lack of a public ctor */
                @Override
                protected void collectInitializationErrors(List<Throwable> errors)
                {
                    super.collectInitializationErrors(errors);
                    errors.removeIf(
                        error -> error.getMessage().contains("Test class should have exactly one public constructor"));
                }
            };

            // Run the test, and rethrow the first failure seen
            List<Failure> failures = new ArrayList<>();

            RunNotifier notifier = new RunNotifier();
            notifier.addFirstListener(new RunListener()
            {
                @Override
                public void testFailure(Failure failure) throws Exception
                {
                    failures.add(failure);
                }
            });
            runner.run(notifier);

            if (!failures.isEmpty())
            {
                throw failures.get(0).getException();
            }
        });
    }

    /** Invoked as a JUnit test method by {@link #simulate} */
    public void _simulate()
    {
        final Consumer<TestRun> testRunUpdater = (testRun) -> {
            if (testRun.getState().finished() &&
                testRun.getFailedDuring() != TestRun.State.CHECKING_RESOURCES)
            {
                testRunnerState.testRunner_addTestRunToFinishedTestRuns(shortTestRunId(testRun.getTestRunId()));
            }
        };

        JobLoggersFactory loggersFactory =
            new JobLoggersFactory(Paths.get(falloutConfiguration().getArtifactPath()), true);

        ActiveTestRunFactory activeTestRunFactory = new ActiveTestRunFactory(falloutConfiguration())
            .withComponentFactory(new TestRunnerTestHelpers.MockingComponentFactory()
                .mockAll(Module.class, () -> new SimulatedModule(testRunnerState))
                .mockAll(Provisioner.class, () -> new SimulatedProvisioner(testRunnerState)))
            .withResourceChecker(SimulatedProvisioner::getMockResChecks);

        try (QueuingTestRunner testRunner =
            new QueuingTestRunner(testRunUpdater,
                new TestRunQueue(new InMemoryPendingQueue(), List::of, blocker,
                    testRunnerState::testRunner_testRunAvailable),
                (testRun) -> new UserCredentials(getTestUser()),
                new ThreadedRunnableExecutorFactory(
                    loggersFactory, testRunUpdater,
                    activeTestRunFactory, falloutConfiguration()),
                testRun -> Collections.emptySet(),
                new ResourceReservationLocks()))
        {
            simulate(testRunner);
        }
    }

    private void simulate(QueuingTestRunner testRunner)
    {
        while (!pendingTestRunPlans.isEmpty() || !runningTestRunPlans.isEmpty())
        {
            debug("---");
            debug("Running %s", runningTestRunPlans.values());

            startTestsOrUnblock(testRunner);

            // Once all the resource checks have failed, then...
            waitForFailedResourceChecks();

            // ...there won't be anything left in the queue that's available, so the
            // runner should block.  We're assuming that when all the resource checks
            // have failed, that means that a) TestRunnerState.testRunner_testRunAvailable
            // will eventually return `false` for all testruns, at which point the testrunner will block.
            waitUntilBlocked();

            waitForRunningTestRuns();
            clearFailedResourceChecks();
            sendTickToTestRunPlans();
            releaseAndWaitForFinishedTestRuns();
        }
        testRunnerState.simulator_checkFinalSimulationState();
    }

    private void startTestsOrUnblock(QueuingTestRunner testRunner)
    {
        List<TestRun> toBeQueued = Collections.emptyList();
        if (!pendingTestRunPlans.isEmpty())
        {
            toBeQueued = getTestRunsForQueueing(testRunner, pendingTestRunPlans.remove());
        }

        // Queuing the testruns will kick the queue, so we must make sure that the check resources
        // results are set _before_ we do that.
        addCheckResourcesMockResults();

        toBeQueued.forEach(testRunner::queueTestRun);

        debug("Queued testruns");

        // If nothing was queued and there are tests waiting for resources, then the queue must be blocked and we
        // need to unblock it.
        if (toBeQueued.isEmpty())
        {
            debug("No tests to start");
            if (runningTestRunPlans.values().stream().anyMatch(TestRunPlan::isWaitingForResources))
            {
                debug("Unblocking queue");
                blocker.unblock();
            }
        }
    }

    private void waitUntilBlocked()
    {
        debug("Wait until blocked");
        blocker.waitUntilBlocked();
    }

    private List<TestRun> getTestRunsForQueueing(QueuingTestRunner testRunner, List<TestRunPlan> testsToStart)
    {
        debug("Preparing to queue %s", testsToStart);

        List<TestRun> toBeQueued = testsToStart.stream().map(testRunPlan -> {
            final TestRun testRun = testRunFactory.makeTestRun(test);
            testRun.setTemplateParamsMap(
                ImmutableMap.of("properties", "test.run.id: " + testRun.getTestRunId().toString()));

            TestRunPlan testRunPlanWithTestRun = testRunPlan.cloneWithTestRun(testRun);
            runningTestRunPlans.put(testRunPlanWithTestRun.getTestRunId(), testRunPlanWithTestRun);
            return testRun;
        }).collect(Collectors.toList());

        return toBeQueued;
    }

    private void addCheckResourcesMockResults()
    {
        Map<Long, Boolean> expectedCheckResources = runningTestRunPlans.values().stream()
            .map(testRunPlan -> Pair.of(testRunPlan.getTestRunId(), testRunPlan.checkResources()))
            .filter(p -> p.getRight().isPresent())
            .collect(Collectors.toMap(p -> p.getLeft(), p -> p.getRight().get()));
        testRunnerState.simulator_addCheckResourcesMockResults(expectedCheckResources);
    }

    /** Waits for <em>failed</em> resource checks only: successful resource checks will lead
     *  to running test runs, which we'll wait for in {@link #waitForRunningTestRuns} */
    private void waitForFailedResourceChecks()
    {
        Set<Long> expectedFailures = runningTestRunPlans.values().stream()
            .filter(testRunPlan -> !testRunPlan.checkResources().orElse(true))
            .map(TestRunPlan::getTestRunId)
            .collect(Collectors.toSet());
        testRunnerState.simulator_waitForFailedResourceChecks(expectedFailures);
    }

    private void clearFailedResourceChecks()
    {
        testRunnerState.simulator_clearFailedResourceChecks();
    }

    private void waitForRunningTestRuns()
    {
        Set<Long> expectedTestRuns = runningTestRunPlans.values().stream()
            .filter(TestRunPlan::isRunning)
            .map(TestRunPlan::getTestRunId)
            .collect(Collectors.toSet());
        testRunnerState.simulator_waitForRunningTestRuns(expectedTestRuns);
    }

    private void sendTickToTestRunPlans()
    {
        runningTestRunPlans.values().forEach(TestRunPlan::tick);
        debug("Tick %s", runningTestRunPlans.values());
    }

    private void releaseAndWaitForFinishedTestRuns()
    {
        Set<Long> testRunsToRelease = runningTestRunPlans.values().stream()
            .filter(TestRunPlan::isFinished)
            .map(TestRunPlan::getTestRunId)
            .collect(Collectors.toSet());

        testRunnerState.simulator_releaseFinishedTestRuns(testRunsToRelease);

        runningTestRunPlans.keySet().removeAll(testRunsToRelease);

        testRunnerState.simulator_waitForFinishedTestRuns(testRunsToRelease);
    }
}
