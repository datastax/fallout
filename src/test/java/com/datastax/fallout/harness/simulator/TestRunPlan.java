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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.quicktheories.core.Gen;

import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

public class TestRunPlan implements Cloneable
{
    private TestRun testRun;
    private int resourcesAvailableAfter;
    private int runFor;

    TestRunPlan(int resourcesAvailableAfter, int runFor)
    {
        this.resourcesAvailableAfter = resourcesAvailableAfter;
        this.runFor = runFor;
    }

    private static Gen<TestRunPlan> anyTestRunPlan()
    {
        // Generate all TestRunPlans of the form TestRunPlan([1..10], [1..10]); resourcesAvailableAfter will be 1
        // 50% of the time (i.e. TestRuns will pass checkResources straight away)
        return integers().between(0, 1).zip(
            integers().between(0, 9),
            integers().between(1, 10), (failsNoResources, noResourcesFor,
                runsFor) -> new TestRunPlan(failsNoResources * noResourcesFor + 1, runsFor));
    }

    /**
     *  QuickTheory Source that generates a list of TestRunPlans for each simulation step
     */
    static Gen<List<TestRunPlan>> testRunPlansForStep()
    {
        // Each step of the simulation will launch between 0 and 5 TestRuns
        Gen<List<TestRunPlan>> allTestRunPlans = lists().of(anyTestRunPlan()).ofSizeBetween(0, 5);

        // Ensure that 50% of the time, we launch 0 TestRuns
        return booleans().all().zip(allTestRunPlans,
            (emptyList, list) -> emptyList ? Collections.<TestRunPlan>emptyList() : list);
    }

    public static long shortTestRunId(UUID testRunId)
    {
        return testRunId.getLeastSignificantBits();
    }

    TestRunPlan cloneWithTestRun(TestRun testRun)
    {
        TestRunPlan testRunPlan;
        try
        {
            testRunPlan = (TestRunPlan) super.clone();
        }
        catch (CloneNotSupportedException e)
        {
            throw new AssertionError(e);
        }
        testRunPlan.testRun = testRun;
        return testRunPlan;
    }

    @Override
    public String toString()
    {
        return String.format("(%s: %s, %s)",
            testRun == null ? "*" : Long.toString(shortTestRunId(testRun.getTestRunId())),
            resourcesAvailableAfter, runFor);
    }

    public Long getTestRunId()
    {
        assertThat(testRun).isNotNull();
        return shortTestRunId(testRun.getTestRunId());
    }

    public void tick()
    {
        QueuingTestRunnerSimulatorTest.assertion("tick-runFor", () -> assertThat(runFor).isGreaterThan(0));

        if (resourcesAvailableAfter > 0)
        {
            --resourcesAvailableAfter;
        }

        if (resourcesAvailableAfter == 0)
        {
            --runFor;
        }
    }

    public Optional<Boolean> checkResources()
    {
        return resourcesAvailableAfter > 0 ?
            Optional.of(resourcesAvailableAfter == 1) :
            Optional.empty();
    }

    // Note that resourcesAvailableAfter == 1 means that resources will become available on the current step
    // _and_ this test run will start running.

    public boolean isWaitingForResources()
    {
        return resourcesAvailableAfter >= 1;
    }

    public boolean isRunning()
    {
        return resourcesAvailableAfter <= 1;
    }

    public boolean isFinished()
    {
        return runFor == 0;
    }
}
