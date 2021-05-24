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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.codahale.metrics.annotation.Timed;
import io.swagger.annotations.Api;

import com.datastax.fallout.runner.QueuingTestRunner;

@Api
@Path("/status")
public class StatusResource
{
    private final QueuingTestRunner testRunner;

    public StatusResource(QueuingTestRunner testRunner)
    {
        this.testRunner = testRunner;
    }

    @GET
    @Path("/allTestsCount")
    @Timed
    @Produces(MediaType.TEXT_PLAIN)
    public int allTestsCount()
    {
        return testRunner.getQueuedTestRuns().size() + testRunner.getRunningTestRunsOrderedByDuration().size();
    }

    @GET
    @Path("/runningTestsCount")
    @Timed
    @Produces(MediaType.TEXT_PLAIN)
    public int runningTestsCount()
    {
        return testRunner.getRunningTestRunsOrderedByDuration().size();
    }
}
