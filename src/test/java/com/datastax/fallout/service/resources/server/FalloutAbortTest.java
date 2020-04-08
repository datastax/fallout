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

import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.FalloutServiceTest;
import com.datastax.fallout.service.resources.RestApiBuilder;

public class FalloutAbortTest extends FalloutServiceTest
{
    @Test
    public void testAbort()
    {
        RestApiBuilder api = FALLOUT_SERVICE_RESET_RULE.userApi();

        //Post yaml
        Response r = api.build("/tests/fakes_sleep/api")
            .put(Entity.entity(EnsembleFalloutTest.readYamlFile("/fakes-sleep.yaml"), "application/yaml"));
        Assert.assertEquals(Response.Status.CREATED.getStatusCode(), r.getStatus());

        //Start test
        r = api.build("/tests/fakes_sleep/runs/api").post(null);
        Assert.assertEquals(Response.Status.CREATED.getStatusCode(), r.getStatus());

        TestRun run = r.readEntity(TestRun.class);

        String statusLocation = r.getLocation().getPath();

        int MAX_LOOP_COUNT = 20; // 20*10 sec ~= 3 minutes
        int loopCount = 0;
        // Wait for test to complete
        while (!run.getState().finished() && loopCount < MAX_LOOP_COUNT)
        {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            run = api.build(statusLocation).get(TestRun.class);

            if (run.getState() == TestRun.State.RUNNING)
            {
                Response d = api.build(statusLocation.replace("/api", "/abort/api")).post(null);
                Assert.assertTrue("got " + d.getStatus(), d.getStatus() == 200);
            }
            loopCount++;
        }
        Assert.assertTrue("Test did not abort in time", loopCount < MAX_LOOP_COUNT);
        Assert.assertEquals(TestRun.State.ABORTED, run.getState());
    }
}
