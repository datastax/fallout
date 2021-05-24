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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.JepsenApi;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.FalloutAppExtensionBase;
import com.datastax.fallout.service.resources.RestApiBuilder;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static javax.ws.rs.core.Response.Status.CREATED;

@Tag("requires-db")
public abstract class TestResourceTestBase<FA extends FalloutAppExtensionBase<?, ?>> extends WithFalloutAppExtension<FA>
{
    protected RestApiBuilder api;

    protected TestResourceTestBase(FA falloutAppExtension)
    {
        super(falloutAppExtension);
    }

    @BeforeAll
    public static void initClojure()
    {
        JepsenApi.preload();
    }

    protected static Response createTest(RestApiBuilder api, String dir, String testName)
    {
        return api.build(TestResource.class, "createTestApi", testName)
            .post(Entity.entity(
                EnsembleFalloutTest.readSharedYamlFile(dir + testName + ".yaml"), "application/yaml"));
    }

    protected Response createTest(String dir, String testName)
    {
        return api.build(TestResource.class, "createTestApi", testName)
            .post(Entity.entity(
                EnsembleFalloutTest.readSharedYamlFile(dir + testName + ".yaml"), "application/yaml"));
    }

    protected Response editTest(String dir, String baseFileName, String testName)
    {
        return api.build(TestResource.class, "createOrUpdateTestApi", testName)
            .put(Entity.entity(
                EnsembleFalloutTest.readSharedYamlFile(dir + baseFileName + ".yaml"), "application/yaml"));
    }

    protected TestRun startTest(RestApiBuilder api, String testName, TestResource.CreateTestRun createTestRun)
    {
        final Response response = api
            .build(TestResource.class, "createTestRunApi", testName)
            .post(Entity.json(createTestRun));

        assertThat(response).hasStatusInfo(CREATED);
        final TestRun testRun = response.readEntity(TestRun.class);
        assertThat(response.getLocation()).hasPath(TestResource.uriForGetTestRunApi(testRun).getPath());
        return testRun;
    }

    private static TestResource.CreateTestRun withYamlTemplateParams(String yamlTemplateParams_)
    {
        return new TestResource.CreateTestRun() {
            {
                this.yamlTemplateParams = yamlTemplateParams_;
            }
        };
    }

    private static TestResource.CreateTestRun withJsonTemplateParams(Map<String, Object> jsonTemplateParams_)
    {
        return new TestResource.CreateTestRun() {
            {
                this.jsonTemplateParams = jsonTemplateParams_;
            }
        };
    }

    private static TestResource.CreateTestRun withNoTemplateParams()
    {
        return null;
    }

    protected TestRun startTest(RestApiBuilder api, String testName, String yamlTemplateParams)
    {
        return startTest(api, testName, withYamlTemplateParams(yamlTemplateParams));
    }

    protected TestRun startTest(String testName, String yamlTemplateParams)
    {
        return startTest(api, testName, withYamlTemplateParams(yamlTemplateParams));
    }

    protected TestRun startTest(RestApiBuilder api, String testName)
    {
        return startTest(api, testName, withNoTemplateParams());
    }

    protected TestRun startTest(String testName)
    {
        return startTest(api, testName, withNoTemplateParams());
    }

    protected TestRun runTest(RestApiBuilder api, String testName, TestResource.CreateTestRun createTestRun)
    {
        final TestRun testRun = startTest(api, testName, createTestRun);

        return waitForCompletedTestRun(testRun);
    }

    protected TestRun runTest(RestApiBuilder api, String testName, String yamlTemplateParams)
    {
        return runTest(api, testName, withYamlTemplateParams(yamlTemplateParams));
    }

    protected TestRun runTest(String testName)
    {
        return runTest(api, testName, withNoTemplateParams());
    }

    protected TestRun runTest(RestApiBuilder api, String testName)
    {
        return runTest(api, testName, withNoTemplateParams());
    }

    protected TestRun runTest(String testName, String yamlTemplateParams)
    {
        return runTest(api, testName, withYamlTemplateParams(yamlTemplateParams));
    }

    protected TestRun runTest(String testName, Map<String, Object> jsonTemplateParams)
    {
        return runTest(api, testName, withJsonTemplateParams(jsonTemplateParams));
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

            assertThat(waitMillis)
                .withFailMessage("Timeout waiting for test runs")
                .isLessThanOrEqualTo(timeout.toMillis());

            testRuns = getTestRunApi(testRuns_);
        }

        return testRuns;
    }

    protected List<TestRun> waitForTestRunsToSatisfy(List<TestRun> testRuns_, Predicate<TestRun> predicate)
    {
        return waitForTestRunsToSatisfy(testRuns_, predicate, Duration.minutes(2));
    }

    protected TestRun waitForCompletedTestRun(TestRun testRun)
    {
        return waitForTestRunsToSatisfy(List.of(testRun),
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
