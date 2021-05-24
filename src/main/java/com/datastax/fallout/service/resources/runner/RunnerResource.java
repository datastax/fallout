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
package com.datastax.fallout.service.resources.runner;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.federecio.dropwizard.swagger.SwaggerResource;
import io.swagger.annotations.Api;

import com.datastax.fallout.FalloutVersion;
import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.harness.TestRunStatusUpdatePublisher.Subscription;
import com.datastax.fallout.runner.DirectTestRunner;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.ServerSentEvents;
import com.datastax.fallout.service.resources.ServerSentEvents.EventSource;
import com.datastax.fallout.service.resources.server.TestResource;
import com.datastax.fallout.service.views.FalloutView;

import static com.datastax.fallout.service.resources.ServerSentEvents.event;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.SERVER_SENT_EVENTS;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Api
@Path("/")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
public class RunnerResource
{
    private final DirectTestRunner testRunner;
    private final ServerSentEvents serverSentEvents;
    private final TestRunStatusUpdatePublisher testRunStatusFeed;

    public RunnerResource(DirectTestRunner testRunner,
        ServerSentEvents serverSentEvents, TestRunStatusUpdatePublisher testRunStatusFeed)
    {
        this.testRunner = testRunner;
        this.serverSentEvents = serverSentEvents;
        this.testRunStatusFeed = testRunStatusFeed;
    }

    @GET
    @Timed
    public Response home()
    {
        return Response.temporaryRedirect(FalloutView.uriFor(SwaggerResource.class)).build();
    }

    public static class RunParams
    {
        @Valid
        @NotNull
        public final TestRun testRun;

        @Valid
        @NotNull
        public final UserCredentials userCredentials;

        public RunParams(@JsonProperty("name") TestRun testRun,
            @JsonProperty("userCredentials") UserCredentials userCredentials)
        {
            this.testRun = testRun;
            this.userCredentials = userCredentials;
        }
    }

    @POST
    @Path("testruns")
    @Timed
    public void run(@NotNull @Valid RunParams runParams)
    {
        if (!testRunner.run(runParams.testRun, runParams.userCredentials))
        {
            throw new WebApplicationException(Response.Status.FORBIDDEN);
        }
    }

    @POST
    @Path("testruns/{testRunId: " + TestResource.ID_PATTERN + "}/abort")
    @Timed
    public void abort(@PathParam("testRunId") UUID testRunId)
    {
        if (!testRunner.abort(testRunId))
        {
            throw new WebApplicationException(NOT_FOUND);
        }
    }

    /** See {@link #statusFeed} */
    public static class AllTestRunStatusesSent
    {
    }

    /** On first connection sends a {@link com.datastax.fallout.runner.TestRunStatusUpdate} for all
     *  known {@link TestRun}s, followed by an {@link AllTestRunStatusesSent} message.  Then it will
     *  send {@link com.datastax.fallout.runner.TestRunStatusUpdate} messages when the state of a {@link TestRun}
     *  changes. */
    @GET
    @Path("status")
    @Produces(SERVER_SENT_EVENTS)
    public void statusFeed(
        @Context HttpServletRequest request, @Context HttpServletResponse response,
        @Context SseEventSink sseEventSink, @Context Sse sse) throws IOException
    {
        serverSentEvents.startEventStream(request, response, sseEventSink, sse, new EventSource() {
            private Optional<Subscription> subscription = Optional.empty();

            @Override
            public void onOpen(ServerSentEvents.EventSink eventSink)
            {
                subscription = Optional.of(testRunStatusFeed
                    .subscribe(testRunStatusUpdate -> eventSink.send(event(sse, testRunStatusUpdate))));
                testRunner.publishCurrentTestRunStatus();
                eventSink.send(event(sse, new AllTestRunStatusesSent()));
            }

            @Override
            public void onClose()
            {
                subscription.ifPresent(Subscription::cancel);
            }
        });
    }

    @POST
    @Path("shutdown")
    @Timed
    public void shutdown()
    {
        testRunner.startShutdown();
    }

    @GET
    @Path("version")
    @Timed
    public String version()
    {
        return FalloutVersion.getVersion();
    }
}
