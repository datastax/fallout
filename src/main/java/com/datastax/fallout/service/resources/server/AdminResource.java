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

import javax.annotation.security.RolesAllowed;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import java.io.PrintStream;
import java.util.List;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import io.swagger.annotations.Api;

import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.QueueAdminTask;
import com.datastax.fallout.service.artifacts.ArtifactUsageAdminTask;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.service.views.MainView;

@Api
@Path("/admin")
@Timed
@RolesAllowed("ADMIN")
public class AdminResource
{
    private final QueuingTestRunner testRunner;
    private final QueueAdminTask queueAdminTask;
    private final MainView mainView;
    private final ArtifactUsageAdminTask artifactUsageAdminTask;

    public AdminResource(QueuingTestRunner testRunner, QueueAdminTask queueAdminTask,
        ArtifactUsageAdminTask artifactUsageAdminTask, MainView mainView)
    {
        this.testRunner = testRunner;
        this.queueAdminTask = queueAdminTask;
        this.mainView = mainView;
        this.artifactUsageAdminTask = artifactUsageAdminTask;
    }

    @GET
    @Path("")
    @Produces(MediaType.TEXT_HTML)
    public FalloutView getAdmin(@Auth User user)
    {
        return new TestAdminView(user);
    }

    @POST
    @Path("/requestShutdown")
    @Produces(MediaType.APPLICATION_JSON)
    public void requestShutdown(@Auth User user)
    {
        queueAdminTask.requestShutdown(user.getEmail());
    }

    @POST
    @Path("/cancelShutdown")
    @Produces(MediaType.APPLICATION_JSON)
    public void cancelShutdown()
    {
        queueAdminTask.cancelShutdown();
    }

    @POST
    @Path("/abortAndRequeueRunningTestRuns")
    @Produces(MediaType.APPLICATION_JSON)
    public List<TestRun> abortAndRequeueRunningTestRuns()
    {
        if (!testRunner.isShutdownRequested())
        {
            throw new WebApplicationException("A shutdown must be requested first", Response.Status.FORBIDDEN);
        }
        if (testRunner.testsHaveBeenAbortedAndRequeued())
        {
            throw new WebApplicationException("Abort and requeue should only be used once while a shutdown is " +
                "requested", Response.Status.FORBIDDEN);
        }

        return testRunner.abortAndRequeueRunningTestRuns();
    }

    @GET
    @Path("/artifactUsage")
    @Produces("text/csv")
    public StreamingOutput artifactUsage()
    {
        return output -> artifactUsageAdminTask.writeArtifactUsage(new PrintStream(output), false);
    }

    public class TestAdminView extends FalloutView
    {
        public TestAdminView(User user)
        {
            super("test-admin.mustache", user, mainView);
        }

        public String getArtifactUsageTaskName()
        {
            return artifactUsageAdminTask.getName();
        }
    }
}
