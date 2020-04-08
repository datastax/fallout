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

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.dropwizard.auth.Auth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.FalloutVersion;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.service.views.LinkedTestRuns;
import com.datastax.fallout.service.views.MainView;

import static com.datastax.fallout.service.views.LinkedTestRuns.TableDisplayOption.*;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class HomeResource
{
    private static final Logger logger = LoggerFactory.getLogger(HomeResource.class);
    private final FalloutConfiguration configuration;
    private final UserDAO userDAO;
    private final TestRunDAO testRunDAO;
    private final QueuingTestRunner testRunner;
    private final MainView mainView;

    public HomeResource(FalloutConfiguration configuration, UserDAO userDAO, TestRunDAO testRunDAO,
        QueuingTestRunner testRunner,
        MainView mainView)
    {
        this.configuration = configuration;
        this.userDAO = userDAO;
        this.testRunDAO = testRunDAO;
        this.testRunner = testRunner;
        this.mainView = mainView;
    }

    @GET
    public Object display(@Auth Optional<User> user)
    {
        if (!user.isPresent())
        {
            URI uri = UriBuilder.fromUri("/a/pages/login.html").build();
            return Response.seeOther(uri).build();
        }
        return new HomeView(user);
    }

    @Path("/version/api")
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public Response versionApi()
    {
        return Response.ok().entity(FalloutVersion.getVersion()).build();
    }

    @Path("/directory")
    @GET
    public DirectoryView directory(@Auth Optional<User> user)
    {
        return new DirectoryView(user);
    }

    public class DirectoryView extends FalloutView
    {
        final List<Map<String, String>> allUsers;

        public DirectoryView(Optional<User> user)
        {
            super("directory.mustache", user, mainView);

            this.allUsers = userDAO.getAllUsers();
            Collections.sort(this.allUsers, Comparator.comparing(userMap -> userMap.get("name")));

        }
    }

    @Path("current_tests")
    @GET
    public CurrentTestsView currentTests(@Auth Optional<User> user)
    {
        return new CurrentTestsView(user);
    }

    public class CurrentTestsView extends FalloutView
    {
        final LinkedTestRuns runningTestRuns;
        final LinkedTestRuns queuedTestRuns;
        final LinkedTestRuns recentTestRuns;
        final List<ResourceRequirement> activeResources;
        final List<ResourceRequirement> requestedResources;

        public CurrentTestsView(Optional<User> user)
        {
            super("current-tests.mustache", user, mainView);
            List<ReadOnlyTestRun> runningTestRuns = testRunner.getRunningTestRunsOrderedByDuration();
            List<ReadOnlyTestRun> queuedTestRuns = testRunner.getQueuedTestRuns();

            this.runningTestRuns = new LinkedTestRuns(user, runningTestRuns)
                .hide(FINISHED_AT, RESULTS, TEMPLATE_PARAMS, DEFINITION, RESTORE_ACTIONS, SIZE_ON_DISK);
            this.queuedTestRuns = new LinkedTestRuns(user, true, queuedTestRuns)
                .hide(FINISHED_AT, RESULTS, TEMPLATE_PARAMS, DEFINITION, RESTORE_ACTIONS, DELETE_MANY, SIZE_ON_DISK);

            recentTestRuns = new LinkedTestRuns(user, testRunDAO.getRecentFinishedTestRuns())
                .hide(RESULTS, TEMPLATE_PARAMS, DEFINITION, MUTATION_ACTIONS, RESTORE_ACTIONS, DELETE_MANY,
                    SIZE_ON_DISK);

            activeResources = TestRun.getResourceRequirementsForTestRuns(runningTestRuns);
            requestedResources = TestRun.getResourceRequirementsForTestRuns(queuedTestRuns);
        }
    }

    public class HomeView extends FalloutView
    {
        public HomeView(Optional<User> user)
        {
            super("home.mustache", user, mainView);
        }
    }
}
