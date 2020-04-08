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
import javax.ws.rs.core.Response;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.datastax.fallout.service.core.PerformanceReport;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.FalloutServiceRule;
import com.datastax.fallout.service.resources.RestApiBuilder;

import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.ResponseAssert.assertThat;

public class PerformanceServiceTest extends TestResourceTestBase
{
    private static final String TEST_NAME = "performance-report-test";

    RestApiBuilder adminApi;

    @ClassRule
    public static final FalloutServiceRule FALLOUT_SERVICE_RULE = new FalloutServiceRule();

    @Rule
    public final FalloutServiceRule.FalloutServiceResetRule FALLOUT_SERVICE_RESET_RULE =
        FALLOUT_SERVICE_RULE.resetRule();

    @Before
    public void setupRestApiBuilders()
    {
        api = FALLOUT_SERVICE_RESET_RULE.userApi();
        adminApi = FALLOUT_SERVICE_RESET_RULE.adminApi();
    }

    @Test
    public void testReportApi()
    {
        // Run a test which will produce HDR files
        assertThat(createTest(api, "/", TEST_NAME)).hasStatusInfo(CREATED);
        TestRun run = runTest(TEST_NAME);

        // Create performance report using HDR files
        PerformanceToolResource.ReportTestPojo args = new PerformanceToolResource.ReportTestPojo("unit-test",
            ImmutableMap.of(run.getTestName(), Lists.newArrayList(run.getTestRunId().toString())));
        Response createReport = api.build("/performance/run/api").post(Entity.json(args));
        assertThat(createReport).hasStatusInfo(CREATED);
        PerformanceReport report = createReport.readEntity(PerformanceReport.class);

        // Check performance report artifacts
        String artifact = report.getReportArtifact();
        Assert.assertTrue(artifact.endsWith(".html"));

        Response json = api.build("/artifacts/" + artifact.substring(0, artifact.lastIndexOf(".")) + ".json").get();
        assertThat(json).hasStatusInfo(OK);
        Assert.assertTrue(json.readEntity(Map.class).size() > 0);

        // Check tests / test runs cannot be deleted while being used by a performance report
        assertThat(api.build(TestResource.class, "deleteTestForUserApi", run.getTestName()).delete())
            .hasStatusInfo(FORBIDDEN);
        assertThat(api.build(TestResource.class, "deleteTestRun", run.getOwner(), run.getTestName(),
            run.getTestRunId().toString()).delete()).hasStatusInfo(FORBIDDEN);

        // Check cloned tests can be deleted while the original is being used by a performance report
        assertThat(createTest(adminApi, "/", TEST_NAME)).hasStatusInfo(CREATED);
        assertThat(adminApi.build(TestResource.class, "deleteTestForUserApi", TEST_NAME)
            .delete()).hasStatusInfo(OK);

        // Check tests / test runs can be deleted after the performance report is deleted
        assertThat(api.build(PerformanceToolResource.class, "deleteReport", report.getEmail(),
            report.getReportGuid().toString()).delete()).hasStatusInfo(OK);
        assertThat(api.build(TestResource.class, "deleteTestRun", run.getOwner(), run.getTestName(),
            run.getTestRunId().toString()).delete()).hasStatusInfo(OK);
        assertThat(api.build(TestResource.class, "deleteTestForUserApi", run.getTestName()).delete()).hasStatusInfo(OK);
        assertThat(api.build("tests/deleted/" + TEST_USER_EMAIL + "/" + TEST_NAME + "/api").get())
            .hasStatusInfo(OK);
    }
}
