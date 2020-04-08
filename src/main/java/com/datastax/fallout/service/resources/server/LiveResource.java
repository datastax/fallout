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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.UUID;

import com.codahale.metrics.annotation.Timed;
import io.swagger.annotations.Api;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.artifacts.ArtifactWatcher;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.resources.ServerSentEvents;
import com.datastax.fallout.service.resources.ServerSentEvents.EventSink;
import com.datastax.fallout.service.resources.ServerSentEvents.EventSource;

import static com.datastax.fallout.service.resources.server.TestResource.EMAIL_PATTERN;
import static com.datastax.fallout.service.resources.server.TestResource.ID_PATTERN;
import static com.datastax.fallout.service.resources.server.TestResource.NAME_PATTERN;
import static com.datastax.fallout.service.views.FalloutView.uriFor;

@Api
@Path("/live")
public class LiveResource
{
    private static final Logger logger = LoggerFactory.getLogger(LiveResource.class);

    private final ArtifactWatcher artifactWatcher;
    private final ServerSentEvents serverSentEvents;
    private final TestRunDAO testRunDAO;

    public LiveResource(TestRunDAO testRunDAO, ArtifactWatcher artifactWatcher, ServerSentEvents serverSentEvents)
    {
        this.testRunDAO = testRunDAO;
        this.artifactWatcher = artifactWatcher;
        this.serverSentEvents = serverSentEvents;
    }

    @GET
    @Path("artifacts/" +
        "{userEmail: " + EMAIL_PATTERN + "}/{name: " + NAME_PATTERN + "}/{testRunId: " + ID_PATTERN + "}/" +
        "{artifactPath: .*}")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @Timed
    public void artifactEventStream(
        @PathParam("userEmail") String email,
        @PathParam("name") String testName,
        @PathParam("testRunId") String testRunId,
        @PathParam("artifactPath") String artifactPathName,
        @Context HttpServletRequest request, @Context HttpServletResponse response,
        @Context SseEventSink eventSink, @Context Sse sse) throws IOException
    {
        final TestRun testRun = testRunDAO.get(email, testName, UUID.fromString(testRunId));

        if (testRun == null)
        {
            throw new WebApplicationException(
                "TestRun does not exist", Response.Status.NOT_FOUND);
        }

        if (testRun.getState().finished())
        {
            serverSentEvents.startEventStream(request, response, eventSink, sse, eventSink_ -> {
                eventSink_.send(sse.newEvent("state", "finished"));
                eventSink_.close();
            });
            return;
        }

        java.nio.file.Path relativeArtifactPath = Paths.get(email, testName, testRunId, artifactPathName);
        if (!artifactWatcher.getAbsolutePath(relativeArtifactPath).toFile().exists())
        {
            throw new WebApplicationException(
                String.format("Artifact %s does not exist", artifactPathName),
                Response.Status.NOT_FOUND);
        }

        serverSentEvents.startEventStream(request, response, eventSink, sse, new EventSource()
        {
            private long watchId;

            private boolean sendStateEvent(EventSink eventSink)
            {
                eventSink.send(sse.newEvent("state", "updated"));

                final TestRun testRun = testRunDAO.get(email, testName, UUID.fromString(testRunId));
                if (testRun.getState().finished())
                {
                    eventSink.send(sse.newEvent("state", "finished"));
                    eventSink.close();
                }

                return !eventSink.isClosed();
            }

            @Override
            public void onOpen(EventSink eventSink)
            {
                if (!sendStateEvent(eventSink))
                {
                    return;
                }
                watchId = artifactWatcher.watch(relativeArtifactPath, ignored -> sendStateEvent(eventSink));
            }

            @Override
            public void onClose()
            {
                artifactWatcher.cancel(watchId);
            }
        });
    }

    public static URI uriForArtifactEventStream(ReadOnlyTestRun testRun, String artifactPath)
    {
        return uriFor(LiveResource.class, "artifactEventStream",
            testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(), artifactPath);
    }
}
