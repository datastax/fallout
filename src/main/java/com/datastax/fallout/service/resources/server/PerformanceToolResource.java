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

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import io.dropwizard.auth.Auth;

import com.datastax.fallout.components.file_artifact_checkers.HdrHistogramChecker;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.service.core.PerformanceReport;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.db.PerformanceReportDAO;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.db.UserGroupMapper;
import com.datastax.fallout.service.views.FalloutView;
import com.datastax.fallout.service.views.LinkedTestRuns;
import com.datastax.fallout.service.views.MainView;

import static com.datastax.fallout.service.resources.server.AccountResource.EMAIL_PATTERN;
import static com.datastax.fallout.service.views.LinkedTestRuns.TableDisplayOption;

@Path("/performance")
public class PerformanceToolResource
{
    final TestDAO testDAO;
    final TestRunDAO testRunDAO;
    final PerformanceReportDAO reportDAO;
    final String rootArtifactPath;
    private final MainView mainView;
    private final UserGroupMapper userGroupMapper;

    public PerformanceToolResource(TestDAO testDAO, TestRunDAO testRunDAO, PerformanceReportDAO reportDAO,
        String rootArtifactPath,
        MainView mainView, UserGroupMapper userGroupMapper)
    {
        this.testDAO = testDAO;
        this.testRunDAO = testRunDAO;
        this.reportDAO = reportDAO;
        this.rootArtifactPath = rootArtifactPath;
        this.mainView = mainView;
        this.userGroupMapper = userGroupMapper;
    }

    @GET
    @Path("{email:" + EMAIL_PATTERN + "}")
    @Produces(MediaType.TEXT_HTML)
    public FalloutView list(@Auth Optional<User> user, @PathParam("email") String email)
    {
        List<PerformanceReport> reportList = reportDAO.getAll(email);
        reportList.sort((c1, c2) -> c2.getReportDate().compareTo(c1.getReportDate()));
        return new ListView(user, email, reportList);
    }

    @GET
    @Path("{email:" + EMAIL_PATTERN + "}/api")
    @Produces(MediaType.APPLICATION_JSON)
    public Response listApi(@Auth Optional<User> user, @PathParam("email") String email)
    {
        List<PerformanceReport> reportList = reportDAO.getAll(email);
        reportList.sort((c1, c2) -> c2.getReportDate().compareTo(c1.getReportDate()));
        return Response.ok(reportList).build();
    }

    @GET
    @Path("{email:" + EMAIL_PATTERN + "}/report/{report:" + TestResource.ID_PATTERN + "}")
    @Produces(MediaType.TEXT_HTML)
    public FalloutView report(@Auth Optional<User> user, @PathParam("email") String email,
        @PathParam("report") String reportId)
    {
        PerformanceReport report = reportDAO.get(email, UUID.fromString(reportId));

        if (report == null)
            throw new WebApplicationException("Report not found");

        List<TestRun> testRuns = report.getReportTestRuns().stream()
            .map(tri -> {
                TestRun tr = testRunDAO.get(tri);
                if (tr != null)
                {
                    return tr;
                }
                return createOwnerlessTestRun(tri);
            })
            .collect(Collectors.toList());

        LinkedTestRuns linkedTestRuns =
            new LinkedTestRuns(userGroupMapper, user, testRuns).hide(TableDisplayOption.MUTATION_ACTIONS,
                TableDisplayOption.RESTORE_ACTIONS);

        return new ReportView(user, report, linkedTestRuns);
    }

    private TestRun createOwnerlessTestRun(TestRunIdentifier tri)
    {
        TestRun ret = new TestRun();
        ret.setTestRunId(tri.getTestRunId());
        ret.setTestName(tri.getTestName());
        ret.setOwner("Unknown - Test Run has been deleted.");
        return ret;
    }

    @GET
    @Path("{email:" + EMAIL_PATTERN + "}/report/{report:" + TestResource.ID_PATTERN + "}/api")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reportApi(@Auth Optional<User> user, @PathParam("email") String email,
        @PathParam("report") String reportId)
    {
        PerformanceReport report = reportDAO.get(email, UUID.fromString(reportId));

        if (report == null)
            return Response.status(Response.Status.BAD_REQUEST).build();

        return Response.ok(report).build();
    }

    @DELETE
    @Path("{email:" + EMAIL_PATTERN + "}/report/{report:" + TestResource.ID_PATTERN + "}/api")
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteReport(@Auth User user, @PathParam("email") String email,
        @PathParam("report") String reportId)
    {
        if (!user.getEmail().equals(email))
            return Response.status(Response.Status.FORBIDDEN).build();

        reportDAO.delete(email, UUID.fromString(reportId));

        return Response.ok().build();
    }

    @GET
    @Path("/start")
    @Produces(MediaType.TEXT_HTML)
    public FalloutView start(@Auth User user)
    {
        return new FalloutView("performance-tool-start.mustache", user, mainView);
    }

    @POST
    @Path("/run/api")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response runApi(@Auth User user, ReportTestPojo args)
    {
        if (args.reportName == null)
            return FalloutView.error("Missing report name argument");

        if (args.reportTests == null || args.reportTests.isEmpty())
            return FalloutView.error("No tests supplied");

        List<TestRun> testRuns = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : args.reportTests.entrySet())
        {
            for (String run : entry.getValue())
            {
                UUID runUid = UUID.fromString(run);

                TestRun testRun = testRunDAO.get(user.getEmail(), entry.getKey(), runUid);

                if (testRun == null)
                    return FalloutView.error(String.format("Missing test run for %s : %s", entry.getKey(), run));

                testRuns.add(testRun);
            }
        }

        try
        {
            PerformanceReport report = createPerformanceReport(testRuns, user.getEmail(), args.reportName);

            URI redirectURI = UriBuilder.fromResource(TestResource.class)
                .path("{email}/report/{report}")
                .build(user.getEmail(), report.getReportGuid());

            return Response.created(redirectURI).entity(report).build();
        }
        catch (Exception e)
        {
            return FalloutView.error(e);
        }
    }

    private PerformanceReport createPerformanceReport(List<TestRun> testRuns, String userEmail, String reportName)
        throws Exception
    {
        HdrHistogramChecker checker = new HdrHistogramChecker();
        UUID reportGuid = UUID.randomUUID();

        Set<TestRunIdentifier> testRunIdentifiers = testRuns.stream()
            .map(ReadOnlyTestRun::getTestRunIdentifier)
            .collect(Collectors.toSet());

        if (testRuns.size() != testRunIdentifiers.size())
        {
            throw new RuntimeException("Test Runs submitted for report contains duplicates");
        }

        String fullReportName =
            Paths.get("performance_reports", userEmail, reportGuid.toString(), "report").toString();

        java.nio.file.Path scratchDir =
            Files.createTempDirectory(String.format("performance-tool-report-%s", reportName));
        try
        {
            checker.compareTests(testRuns, rootArtifactPath, fullReportName, reportName, scratchDir);
        }
        finally
        {
            FileUtils.deleteDir(scratchDir);
        }

        PerformanceReport report = new PerformanceReport();
        report.setEmail(userEmail);
        report.setReportName(reportName);
        report.setReportArtifact(fullReportName + ".html");
        report.setReportTestRuns(testRunIdentifiers);
        report.setReportDate(new Date());
        report.setReportGuid(reportGuid);

        reportDAO.add(report);

        return report;
    }

    @POST
    @Path("/run")
    @Produces(MediaType.APPLICATION_JSON)
    public Response run(@Auth User user, @FormParam("report-name") String reportName, Form formData)
    {
        if (reportName == null || reportName.trim().isEmpty())
        {
            return FalloutView.error("Report name is required");
        }

        MultivaluedMap<String, String> formMap = formData.asMap();
        List<TestRun> args = new ArrayList<>();
        Set<UUID> uniqueIds = new HashSet<>();

        for (int i = 1; i < 1000; i++)
        {
            String userFormId = String.format("user-%d", i);
            List<String> userEmailList = formMap.getOrDefault(userFormId, List.of());
            if (userEmailList.size() > 1)
            {
                return FalloutView.error(">1 user was found under: " + userFormId);
            }
            if (userEmailList.isEmpty() || userEmailList.get(0).trim().isEmpty())
            {
                break;
            }

            String userEmail = userEmailList.get(0).trim();

            String testFormId = String.format("test-name-%d", i);
            List<String> testNameList = formMap.getOrDefault(testFormId, List.of());
            if (testNameList.size() > 1)
            {
                return FalloutView.error(">1 name was found under: " + testFormId);
            }

            String testName = testNameList.get(0).trim();

            String testRunFormId = String.format("test-run-%d", i);
            List<String> testRunIdList = formMap.getOrDefault(testRunFormId, List.of());

            if (testRunIdList.isEmpty())
            {
                break;
            }

            String run = testRunIdList.get(0).trim();
            if (run.isEmpty())
            {
                break;
            }

            UUID runUid = UUID.fromString(run);
            if (!uniqueIds.add(runUid))
            {
                return FalloutView.error("Duplicate test runs specified.");
            }

            TestRun arg = testRunDAO.get(userEmail, testName, runUid);
            if (arg == null)
            {
                return FalloutView.error(String.format("Missing TestRun %s %s", testName, run));
            }

            args.add(arg);
        }

        if (args.isEmpty())
        {
            return FalloutView.error("At least 1 test run required");
        }

        try
        {
            createPerformanceReport(args, user.getEmail(), reportName);
        }
        catch (Exception e)
        {
            return FalloutView.error(e);
        }

        return Response.ok().build();
    }

    class ListView extends FalloutView
    {
        public final String email;
        public final List<PerformanceReport> reports;

        public ListView(Optional<User> user, String email, List<PerformanceReport> reports)
        {
            super("performance-tool-list.mustache", user, mainView);
            this.email = email;
            this.reports = reports;
        }

        public boolean isOwner()
        {
            return user != null && user.getEmail().equals(email);
        }
    }

    class ReportView extends FalloutView
    {
        public final PerformanceReport report;
        public final LinkedTestRuns linkedTestRuns;

        public ReportView(Optional<User> user, PerformanceReport report, LinkedTestRuns linkedTestRuns)
        {
            super("performance-tool-report.mustache", user, mainView);
            this.report = report;
            this.linkedTestRuns = linkedTestRuns;
        }
    }

    public static class ReportTestPojo
    {
        public final String reportName;
        public final Map<String, List<String>> reportTests;

        public ReportTestPojo(String name, Map<String, List<String>> testruns)
        {
            this.reportName = name;
            this.reportTests = testruns;
        }

        //For JSON Marshalling
        ReportTestPojo()
        {
            this.reportName = null;
            this.reportTests = null;
        }
    }

}
