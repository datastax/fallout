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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.harness.JepsenApi;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;

import static com.datastax.fallout.assertj.Assertions.fail;
import static com.datastax.fallout.harness.simulator.Simulator.debug;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.lists;

/**
 * The purpose of this test is to test the behaviour of {@link QueuingTestRunner} when presented with several
 * independent batches of TestRuns.  Sensing and control of what is happening is achieved by mocking a
 * Provisioner ({@link Simulator.SimulatedProvisioner}), a Module ({@link Simulator.SimulatedModule}) and the unblock timer of
 * {@link TestRunQueue} ({@link Simulator.SimulatedUnblockTimer}). Coordination between the simulation and the
 * sensed state is managed by {@link TestRunnerState}.  Predicted state is held in {@link TestRunPlan}.
 *
 * <p>The test uses <a href="https://github.com/NCR-CoDE/QuickTheories">QuickTheory</a> to generate multiple
 * random simulator runs. The usage is slightly unorthodox, as property-based testing frameworks tend to assume
 * that we're testing only one property in each test (this assumption makes shrinking the inputs to find a minimum
 * failing input possible), and we're making multiple assertions in each test.  To get around this, we ignore
 * all but the failing assertion when shrinking: see {@link #resetShrinkingAssertions()} and {@link #assertion}.
 */

// TODO: is this doc still relevant with junit5 instead of junit4?
/** This doesn't appear to kill all the threads, or even the one running the test (it's only interrupted)
 * This means that this test really needs to be run in a fork-per-class
 * or (better) fork-per-method runner (which is a performance hit). */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
public class QueuingTestRunnerSimulatorTest extends WithPersistentTestOutputDir
{

    private static volatile Optional<String> shrinkingForAssertion = Optional.empty();

    private static void resetShrinkingAssertions()
    {
        shrinkingForAssertion = Optional.empty();
    }

    static void assertion(String name, Runnable r)
    {
        try
        {
            if (shrinkingForAssertion.map(assertionName -> assertionName.equals(name)).orElse(true))
            {
                r.run();
            }
            else
            {
                debug("shrinking; ignoring assertion %s", name);
            }
        }
        catch (Throwable t)
        {
            debug("assertion %s failed: %s", name, t);
            shrinkingForAssertion = Optional.of(name);
            throw t;
        }
    }

    static void waitTimedOut(String name)
    {
        assertion("wait." + name, () -> fail(name + " timed out"));
    }

    private int simulationId = 0;

    /**
     *  Static Clojure initialisation within ActiveTestRun takes a few seconds: if we allow initialisation to happen
     *  within the simulation, it requires a larger value for {@link Simulator#SYNCHRONIZATION_TIMEOUT}
     *  than necessary, which makes shrinking slower.  To prevent this, we initialize it here.
     */
    @BeforeAll
    public static void initClojure()
    {
        JepsenApi.preload();
    }

    @org.junit.jupiter.api.Test
    public void runManySimulations()
    {
        TestHelpers.setTestRunArtifactPath(persistentTestOutputDir());
        resetShrinkingAssertions();
        qt()
            .withFixedSeed(24690760670735L)
            .withExamples(30)
            .withShrinkCycles(100)
            .forAll(lists().of(TestRunPlan.testRunPlansForStep()).ofSizeBetween(1, 50))
            .checkAssert(testsToStart -> new Simulator(++simulationId, testsToStart).simulate());
    }

    // The remaining tests are here to enable debugging failed results from runManySimulations

    private static TestRunPlan p(int noResourcesFor, int runFor)
    {
        return new TestRunPlan(noResourcesFor, runFor);
    }

    private static List<TestRunPlan> p(TestRunPlan... ps)
    {
        return List.of(ps);
    }

    @org.junit.jupiter.api.Test
    public void runOneSimulation()
    {
        TestHelpers.setTestRunArtifactPath(persistentTestOutputDir());
        new Simulator(++simulationId, List.of(
            p(p(1, 8)))).simulate();
    }
}
