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

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.datastax.fallout.harness.TestResult;
import com.datastax.fallout.harness.TestRunAbortedStatusUpdater;
import com.datastax.fallout.service.core.TestRun;

public class TestRunStateStorage implements TestRunAbortedStatusUpdater.StateStorage
{
    private final AtomicallyPersistedTestRun testRun;
    private final Logger logger;
    private final Supplier<Map<String, Long>> findTestRunArtifacts;

    public TestRunStateStorage(AtomicallyPersistedTestRun testRun, Logger logger,
        Supplier<Map<String, Long>> findTestRunArtifacts)
    {
        this.logger = logger;
        this.testRun = testRun;
        this.findTestRunArtifacts = findTestRunArtifacts;
    }

    @Override
    public void setCurrentState(TestRun.State state)
    {
        testRun.update(testRun -> {
            logger.info("Test run state: {} -> {}", testRun.getState(), state);
            testRun.setState(state);
            if (state == TestRun.State.PREPARING_RUN)
            {
                testRun.start();
            }
        });
    }

    @Override
    public TestRun.State getCurrentState()
    {
        return testRun.get(TestRun::getState);
    }

    @Override
    public void markFailedWithReason(TestRun.State finalState)
    {
        if (finalState == TestRun.State.ABORTED)
        {
            logger.warn("Test run aborted by user");
        }

        testRun.update(testRun -> {
            if (testRun.getFailedDuring() == null)
            {
                testRun.setFailedDuring(testRun.getState());
            }
        });
    }

    @Override
    public void markInactive(TestRun.State finalState,
        Optional<TestResult> testResult)
    {
        String results = testResult.map(tr -> tr.results().toString())
            .orElse("No Jepsen results: check logs for error");

        testRun.update(testRun -> {
            testRun.setResults(results);
            testRun.setFinishedAt(new Date());
            testRun.setState(finalState);

            logger.info("Test run completed for {}", testRun.getTestRunId().toString());

            try
            {
                testRun.updateArtifacts(findTestRunArtifacts.get());
            }
            catch (Throwable e)
            {
                logger.error("Failed to set artifacts", e);
                testRun.setState(TestRun.State.FAILED);
            }
        });
    }
}
