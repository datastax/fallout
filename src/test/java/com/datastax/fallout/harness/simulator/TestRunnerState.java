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
package com.datastax.fallout.harness.simulator;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.harness.simulator.Simulator.SYNCHRONIZATION_TIMEOUT;
import static com.datastax.fallout.harness.simulator.Simulator.debug;

/**
 * Controls and senses the state of the TestRunner.  Methods prefixed with simulator_ are called by the simulator to
 * control the TestRunner and set expectations; methods prefixed with testRunner_ are called by the TestRunner to
 * get mocked results and report state.
 */
public class TestRunnerState implements AutoCloseable
{
    private final Guarded<Map<Long, Boolean>> checkResourcesMockedResults =
        new Guarded<>(new HashMap<>(), "checkResourcesMockedResults");
    private final Guarded<Set<Long>> failedResourceChecks =
        new Guarded<>(new HashSet<>(), "failedResourceChecks");
    private final Guarded<Set<Long>> runningTestRuns =
        new Guarded<>(new HashSet<>(), "runningTestRuns");
    private final Guarded<Set<Long>> runningTestRunsForRelease =
        new Guarded<>(new HashSet<>(), "runningTestRunsForRelease");
    private final Guarded<Set<Long>> finishedTestRuns =
        new Guarded<>(new HashSet<>(), "finishedTestRuns");

    private volatile boolean shutdownRequested = false;

    private String withShutdownRequestedStr(String message)
    {
        return message + (shutdownRequested ? " [shutdownRequested]" : "");
    }

    public void close()
    {
        shutdownRequested = true;
        checkResourcesMockedResults.notifyAfter().noop();
        runningTestRuns.notifyAfter().noop();
        runningTestRunsForRelease.notifyAfter().noop();
        finishedTestRuns.notifyAfter().noop();
    }

    void simulator_waitForRunningTestRuns(Set<Long> expectedTestRuns)
    {
        runningTestRuns
            .waitUntil(testRuns -> testRuns.containsAll(expectedTestRuns))
            .withPredicateDescription(String.format("containsAll %s", expectedTestRuns))
            .withTimeout(SYNCHRONIZATION_TIMEOUT, QueuingTestRunnerSimulatorTest::waitTimedOut)
            .accept(testRuns -> QueuingTestRunnerSimulatorTest.assertion("waitForRunningTestRuns",
                () -> assertThat(testRuns).isEqualTo(expectedTestRuns)));

        checkResourcesMockedResults
            .notifyAfter()
            .accept(mockedResults -> mockedResults.keySet().removeAll(expectedTestRuns));
    }

    void simulator_releaseFinishedTestRuns(Collection<Long> testRunsToRelease)
    {
        runningTestRuns
            .notifyAfter()
            .withOperationDescription(
                String.format("removeAll %s", testRunsToRelease))
            .accept(testRuns -> testRuns.removeAll(testRunsToRelease));

        runningTestRunsForRelease
            .notifyAfter()
            .withOperationDescription(
                String.format("addAll %s", testRunsToRelease))
            .accept(testRuns -> testRuns.addAll(testRunsToRelease));
    }

    void testRunner_addTestRunToWaitingTestRunsAndWaitForRelease(long testRunId)
    {
        runningTestRuns
            .notifyAfter()
            .accept(testRuns -> testRuns.add(testRunId));

        runningTestRunsForRelease
            .waitUntil(testRuns -> shutdownRequested || testRuns.contains(testRunId))
            .withPredicateDescription(withShutdownRequestedStr(String.format("contains %s", testRunId)))
            .withOperationDescription(
                String.format("remove %s", testRunId))
            .accept(testRuns -> testRuns.remove(testRunId));
    }

    void simulator_waitForFinishedTestRuns(Set<Long> expectedFinishedTestRuns)
    {
        finishedTestRuns
            .waitUntil(testRuns -> testRuns.containsAll(expectedFinishedTestRuns))
            .withPredicateDescription(String.format("containsAll %s", expectedFinishedTestRuns))
            .withTimeout(SYNCHRONIZATION_TIMEOUT, QueuingTestRunnerSimulatorTest::waitTimedOut)
            .accept(testRuns -> {
                QueuingTestRunnerSimulatorTest.assertion("waitForFinishedTestRuns",
                    () -> assertThat(testRuns).isEqualTo(expectedFinishedTestRuns));
                testRuns.clear();
            });
    }

    public void testRunner_addTestRunToFinishedTestRuns(long testRunId)
    {
        finishedTestRuns
            .notifyAfter()
            .withOperationDescription(
                String.format("add(%s)", testRunId))
            .accept(finished -> finished.add(testRunId));
    }

    void simulator_addCheckResourcesMockResults(Map<Long, Boolean> results)
    {
        checkResourcesMockedResults
            .notifyAfter()
            .withOperationDescription(String.format("putAll %s", results))
            .accept(mockedResults -> {
                mockedResults.putAll(results);
            });
    }

    boolean testRunner_checkResources(long testRunId)
    {
        debug("checkResources(%s)...", testRunId);

        boolean resourcesAvailable = checkResourcesMockedResults
            .waitUntil(mockedResults -> shutdownRequested || mockedResults.containsKey(testRunId))
            .withPredicateDescription(withShutdownRequestedStr(String.format("containsKey %s", testRunId)))
            .withOperationDescription(String.format("checkResources %s", testRunId))
            .apply(mockedResults -> {
                boolean result = !shutdownRequested && mockedResults.get(testRunId);
                return result;
            });

        if (!resourcesAvailable)
        {
            failedResourceChecks
                .notifyAfter()
                .withOperationDescription(String.format("add %s", testRunId))
                .accept(failedChecks -> failedChecks.add(testRunId));
        }

        return resourcesAvailable;
    }

    boolean testRunner_testRunAvailable(ReadOnlyTestRun testRun)
    {
        long shortTestRunId = TestRunPlan.shortTestRunId(testRun.getTestRunId());

        return failedResourceChecks
            .guard()
            .withOperationDescription(String.format("testRunAvailable(%s)", shortTestRunId))
            .apply(failedResourceChecks -> testRun.getState() != TestRun.State.WAITING_FOR_RESOURCES ||
                !failedResourceChecks.contains(TestRunPlan.shortTestRunId(testRun.getTestRunId())));
    }

    /** Waits until we've seen at least one failure from each of the expectedFailures.
     *  We use "at least one" because each failure will result in the test run being
     * requeued, and we may see it again whilst waiting for all the failures.  */
    void simulator_waitForFailedResourceChecks(Set<Long> expectedFailures)
    {
        failedResourceChecks
            .waitUntil(failedChecks -> failedChecks.containsAll(expectedFailures))
            .withPredicateDescription(String.format("containsAll %s", expectedFailures))
            .withTimeout(SYNCHRONIZATION_TIMEOUT, QueuingTestRunnerSimulatorTest::waitTimedOut)
            .noop();
    }

    void simulator_clearFailedResourceChecks()
    {
        failedResourceChecks.guard().withOperationDescription("clear").accept(Set::clear);
    }

    void simulator_checkFinalSimulationState()
    {
        checkResourcesMockedResults.accept(testRuns -> QueuingTestRunnerSimulatorTest.assertion(
            "checkFinalSimulationState: checkResourcesMockedResults", () -> assertThat(testRuns).isEmpty()));

        runningTestRuns.accept(testRuns -> QueuingTestRunnerSimulatorTest.assertion(
            "checkFinalSimulationState: runningTestRuns", () -> assertThat(testRuns).isEmpty()));

        runningTestRunsForRelease.accept(testRuns -> QueuingTestRunnerSimulatorTest.assertion(
            "checkFinalSimulationState: runningTestRunsForRelease", () -> assertThat(testRuns).isEmpty()));
    }
}
