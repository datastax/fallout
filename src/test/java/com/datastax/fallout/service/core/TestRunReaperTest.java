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
package com.datastax.fallout.service.core;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import com.datastax.driver.core.Session;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.PerformanceReportDAO;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.service.db.UserGroupMapper;
import com.datastax.fallout.util.NullUserMessenger;
import com.datastax.fallout.util.UserMessenger;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.core.Fakes.TEST_NAME;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;

@Tag("requires-db")
public class TestRunReaperTest
{
    private static TestRunDAO testRunDAO;
    private static PerformanceReportDAO reportDAO;
    private static TestDAO testDAO;
    private static UserMessenger userMessenger;
    private static Session session;
    private final static String externalUrl = "http://localhost:8080";
    private static final String keyspace = "test_run_reaper";
    private static final String testRunId = "69A38F36-8A91-4ABB-A2C8-B669189FEFD5";
    private Test test;
    private TestRun testRun;

    private static final java.time.Duration FOUR_YEARS = java.time.Duration.ofDays(1461);

    @BeforeAll
    public static void startCassandra() throws Exception
    {
        CassandraDriverManager driverManager = createDriverManager(keyspace);

        testRunDAO = new TestRunDAO(driverManager);
        testDAO = new TestDAO(driverManager, testRunDAO);
        reportDAO = new PerformanceReportDAO(driverManager);
        UserDAO userDAO = new UserDAO(driverManager, new SecurityUtil(), Optional.empty(), UserGroupMapper.empty());
        userMessenger = new NullUserMessenger();

        driverManager.start();
        testRunDAO.start();
        testDAO.start();
        reportDAO.start();
        userDAO.start();

        session = driverManager.getSession();
        session.execute(String.format("USE %s", keyspace));
        userDAO.createUserIfNotExists("owner", TEST_USER_EMAIL, "", UserGroupMapper.UserGroup.OTHER);
    }

    @BeforeEach
    public void setup()
    {
        session.execute("TRUNCATE tests");
        session.execute("TRUNCATE test_runs");
        session.execute("TRUNCATE deleted_test_runs");
        session.execute("TRUNCATE performance_reports");

        test = Test.createTest(TEST_USER_EMAIL, TEST_NAME, null);
        testDAO.update(test);
    }

    private void createTestRun(String testName, Date finishedAt, boolean keepForever)
    {
        TestRun testRun = new TestRun();
        testRun.setOwner(TEST_USER_EMAIL);
        testRun.setTestName(testName);
        testRun.setTestRunId(UUID.fromString(testRunId));
        testRun.setState(TestRun.State.PASSED);
        testRun.setFinishedAt(finishedAt);
        testRun.setKeepForever(keepForever);
        testRunDAO.update(testRun);

        assertThat(testRunDAO.getDeleted(TEST_USER_EMAIL, test.getName(), UUID.fromString(testRunId))).isEqualTo(null);

        this.testRun = testRun;
    }

    @org.junit.jupiter.api.Test
    public void test_old_test_run_with_keep_forever_are_not_reaped()
    {
        createTestRun(test.getName(), Date.from(Instant.now().minus(FOUR_YEARS)), true);

        TestRunReaper
            .checkForTestRunsPastTTL(testRunDAO, reportDAO, testDAO, userMessenger, userMessenger, externalUrl,
                () -> false);

        assertThat(testRunDAO.getDeleted(TEST_USER_EMAIL, test.getName(), UUID.fromString(testRunId))).isEqualTo(null);
    }

    @org.junit.jupiter.api.Test
    public void test_test_runs_younger_than_ttl_are_not_reaped()
    {
        createTestRun(test.getName(), Date.from(Instant.now().minus(Duration.ofDays(200))), false);

        TestRunReaper
            .checkForTestRunsPastTTL(testRunDAO, reportDAO, testDAO, userMessenger, userMessenger, externalUrl,
                () -> false);

        assertThat(testRunDAO.getDeleted(TEST_USER_EMAIL, test.getName(), UUID.fromString(testRunId))).isEqualTo(null);
    }

    @org.junit.jupiter.api.Test
    public void test_test_runs_used_in_performance_reports_are_not_reaped()
    {
        createTestRun(test.getName(), Date.from(Instant.now().minus(FOUR_YEARS)), false);

        PerformanceReport fakePerfReport = new PerformanceReport();
        fakePerfReport.setEmail(TEST_USER_EMAIL);
        fakePerfReport.setReportName("fake_report");
        fakePerfReport.setReportDate(new Date());
        fakePerfReport.setReportTestRuns(Set.of(testRun.getTestRunIdentifier()));
        fakePerfReport.setReportGuid(UUID.randomUUID());
        reportDAO.add(fakePerfReport);

        TestRunReaper
            .checkForTestRunsPastTTL(testRunDAO, reportDAO, testDAO, userMessenger, userMessenger, externalUrl,
                () -> false);

        assertThat(testRunDAO.getDeleted(TEST_USER_EMAIL, test.getName(), UUID.fromString(testRunId))).isEqualTo(null);
    }

    @org.junit.jupiter.api.Test
    public void test_old_test_run_is_reaped()
    {
        createTestRun(test.getName(), Date.from(Instant.now().minus(FOUR_YEARS)), false);

        TestRunReaper
            .checkForTestRunsPastTTL(testRunDAO, reportDAO, testDAO, userMessenger, userMessenger, externalUrl,
                () -> false);

        DeletedTestRun deletedTestRun = testRunDAO
            .getDeleted(TEST_USER_EMAIL, test.getName(), UUID.fromString(testRunId));
        assertThat(deletedTestRun).isEqualTo(DeletedTestRun.fromTestRun(testRun));
    }
}
