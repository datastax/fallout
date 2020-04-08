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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.assertj.core.api.Assertions;
import org.junit.experimental.categories.Category;

import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.RestApiBuilder;
import com.datastax.fallout.test.utils.categories.RequiresDb;
import com.datastax.fallout.util.Duration;

import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.ResponseAssert.assertThat;

@Category(RequiresDb.class)
public abstract class TestResourceTestBase
{
    protected RestApiBuilder api;

    protected static Response createTest(RestApiBuilder api, String dir, String testName)
    {
        return api.build(TestResource.class, "createTestApi", testName)
            .post(Entity.entity(
                EnsembleFalloutTest.readYamlFile(dir + testName + ".yaml"), "application/yaml"));
    }

    protected Response createTest(String dir, String testName)
    {
        return api.build(TestResource.class, "createTestApi", testName)
            .post(Entity.entity(
                EnsembleFalloutTest.readYamlFile(dir + testName + ".yaml"), "application/yaml"));
    }

    protected Response editTest(String dir, String baseFileName, String testName)
    {
        return api.build(TestResource.class, "createOrUpdateTestApi", testName)
            .put(Entity.entity(
                EnsembleFalloutTest.readYamlFile(dir + baseFileName + ".yaml"), "application/yaml"));
    }

    protected TestRun startTest(String testName, TestResource.CreateTestRun createTestRun)
    {
        final Response response = api
            .build(TestResource.class, "createTestRunApi", testName)
            .post(Entity.json(createTestRun));

        assertThat(response).hasStatusInfo(CREATED);
        final TestRun testRun = response.readEntity(TestRun.class);
        Assertions.assertThat(response.getLocation()).hasPath(TestResource.uriForGetTestRunApi(testRun).getPath());
        return testRun;
    }

    protected TestRun startTest(String testName, String yamlTemplateParams_)
    {
        return startTest(testName, yamlTemplateParams_ == null ? null :
            new TestResource.CreateTestRun()
            {
                {
                    this.yamlTemplateParams = yamlTemplateParams_;
                }
            });
    }

    protected TestRun startTest(String testName)
    {
        return startTest(testName, (TestResource.CreateTestRun) null);
    }

    protected TestRun runTest(String testName)
    {
        return runTest(testName, (String) null);
    }

    protected TestRun runTest(String testName, String yamlTemplateParams)
    {
        final TestRun testRun = startTest(testName, yamlTemplateParams);

        return waitForCompletedTestRun(testRun);
    }

    protected TestRun runTest(String testName, Map<String, Object> jsonTemplateParams_)
    {
        final TestRun testRun = startTest(testName,
            new TestResource.CreateTestRun()
            {
                {
                    this.jsonTemplateParams = jsonTemplateParams_;
                }
            });

        return waitForCompletedTestRun(testRun);
    }

    protected TestRun reRunTestWithClone(TestRun testRun, boolean clone)
    {
        TestResource.RerunParams rerunParams = new TestResource.RerunParams();
        rerunParams.clone = clone;

        final Response response = api
            .build(TestResource.class, "createTestRerunApi",
                testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId().toString())
            .post(Entity.entity(rerunParams, MediaType.APPLICATION_JSON));

        assertThat(response).hasStatusInfo(CREATED);

        return waitForCompletedTestRun(response.readEntity(TestRun.class));
    }

    private List<TestRun> waitForTestRunsToSatisfy(
        List<TestRun> testRuns_, Predicate<TestRun> predicate, Duration timeout)
    {
        final long sleepDurationMillis = 500;
        long waitMillis = 0;

        List<TestRun> testRuns = getTestRunApi(testRuns_);

        while (!testRuns.stream().allMatch(predicate))
        {
            Uninterruptibles.sleepUninterruptibly(sleepDurationMillis, TimeUnit.MILLISECONDS);
            waitMillis += sleepDurationMillis;

            Assertions.assertThat(waitMillis)
                .withFailMessage("Timeout waiting for test runs")
                .isLessThanOrEqualTo(timeout.toMillis());

            testRuns = getTestRunApi(testRuns_);
        }

        return testRuns;
    }

    protected List<TestRun> waitForTestRunsToSatisfy(List<TestRun> testRuns_, Predicate<TestRun> predicate)
    {
        return waitForTestRunsToSatisfy(testRuns_, predicate, Duration.minutes(6));
    }

    protected TestRun waitForCompletedTestRun(TestRun testRun)
    {
        return waitForTestRunsToSatisfy(Collections.singletonList(testRun),
            testRun_ -> testRun_.getState().finished()).get(0);
    }

    private TestRun getTestRunApi(TestRun testRun)
    {
        return api.build(TestResource.uriForGetTestRunApi(testRun).getPath()).get(TestRun.class);
    }

    private List<TestRun> getTestRunApi(List<TestRun> testRuns)
    {
        return testRuns.stream().map(this::getTestRunApi).collect(Collectors.toList());
    }
}
