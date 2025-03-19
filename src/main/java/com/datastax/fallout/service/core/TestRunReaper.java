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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import io.netty.util.HashedWheelTimer;

import com.datastax.fallout.cassandra.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastax.fallout.service.db.PerformanceReportDAO;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.db.UserGroupMapper;
import com.datastax.fallout.service.views.LinkedTestRuns;
import com.datastax.fallout.util.DateUtils;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ResourceUtils;
import com.datastax.fallout.util.ScopedLogger;
import com.datastax.fallout.util.messenger.UserMessenger;

import static com.datastax.fallout.util.MustacheFactoryWithoutHTMLEscaping.renderWithScopes;

/**
 * Finds and deletes test runs older than 3 years. Affected users are notified with an email informing them they have 30
 * days to save the test runs from the trash bin before permanent deletion
 */
public class TestRunReaper extends PeriodicTask
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(TestRunReaper.class);
    private final TestRunDAO testRunDAO;
    private final PerformanceReportDAO reportDAO;
    private final TestDAO testDAO;
    private final UserMessenger htmlUserMessenger;
    private final UserMessenger slackUserMessenger;
    private final String externalUrl;
    private static final java.time.Duration THREE_YEARS = java.time.Duration.ofDays(1095);

    public TestRunReaper(boolean startPaused, HashedWheelTimer timer,
        ReentrantLock runningTaskLock, Duration delay, Duration repeat, TestRunDAO testRunDAO,
        PerformanceReportDAO reportDAO, TestDAO testDAO, UserMessenger htmlUserMessenger,
        UserMessenger slackUserMessenger, String externalUrl)
    {
        super(startPaused, timer, runningTaskLock, delay, repeat);
        this.testRunDAO = testRunDAO;
        this.reportDAO = reportDAO;
        this.testDAO = testDAO;
        this.htmlUserMessenger = htmlUserMessenger;
        this.slackUserMessenger = slackUserMessenger;
        this.externalUrl = externalUrl;
    }

    @Override
    protected ScopedLogger logger()
    {
        return logger;
    }

    private static void notifyOwner(String owner, UserMessenger userMessenger, String subject, String message)
    {
        logger.withScopedInfo("Notifying {} of deleted test runs via {}", owner, userMessenger.getClass()).run(() -> {
            try
            {
                userMessenger.sendMessage(owner, subject, message);
            }
            catch (UserMessenger.MessengerException e)
            {
                logger.error("{} Failed to notify user {} of deleted test runs {}", userMessenger.getClass(), owner, e);
            }
        });
    }

    private static void deleteTestRunsAndNotifyOwner(String owner, List<TestRun> testRunsForDeletion, TestDAO testDAO,
        UserMessenger htmlUserMessenger, UserMessenger slackMessenger, String externalUrl)
    {
        Map<String, List<TestRun>> testRunsByTest = new TreeMap<>();
        testRunsForDeletion.forEach(testRun -> {
            try
            {
                logger.info("deleting test run {} {}", testRun.getShortName(),
                    DateUtils.formatUTCDate(testRun.getFinishedAt()));
                testDAO.deleteTestRun(testRun);
                testRunsByTest.computeIfAbsent(testRun.getTestName(), k -> new ArrayList<>())
                    .add(testRun);
            }
            catch (Exception e)
            {
                logger.error("Error while deleting test runs", e);
            }
        });

        Map<String, LinkedTestRuns> reapedTestNotificationDetails = new TreeMap<>();
        for (Map.Entry<String, List<TestRun>> entry : testRunsByTest.entrySet())
        {
            var testRuns = entry.getValue();
            testRuns.sort(Comparator.comparing(TestRun::getFinishedAt));
            reapedTestNotificationDetails
                .put(entry.getKey(),
                    new LinkedTestRuns(UserGroupMapper.empty(), Optional.empty(), testRuns, externalUrl));
        }

        var emailDefinition = ResourceUtils
            .getResourceAsString(TestRunReaper.class, "usernotifier-testruns-deleted.email.mustache");

        var slackDefinition = ResourceUtils
            .getResourceAsString(TestRunReaper.class, "usernotifier-testruns-deleted.slack.mustache");

        notifyOwner(owner, htmlUserMessenger, "ALERT: Old fallout test runs marked for deletion",
            renderWithScopes(emailDefinition,
                List.of(
                    Map.of("reapedTestNotificationDetails", new ArrayList<>(reapedTestNotificationDetails.entrySet()),
                        "externalUrl", externalUrl))));

        notifyOwner(owner, slackMessenger, null, renderWithScopes(slackDefinition,
            List.of(Map.of("externalUrl", externalUrl))));
    }

    /**
     * Returns true if the test run older than three years, not marked as keep forever, and not used in performance
     * reports
     */
    private static boolean testRunCanBeDeleted(TestRun testRun, Instant lowerBound, PerformanceReportDAO reportDAO)
    {
        return !testRun.keepForever() && testRun.getState() != null && testRun.getState().finished() &&
            testRun.getFinishedAt() != null &&
            testRun.getFinishedAt().toInstant().isBefore(lowerBound) &&
            PerformanceReportDAO.getPerformanceReportsContainingTestRun(reportDAO,
                tri -> tri.getTestRunId().equals(testRun.getTestRunId())).isEmpty();
    }

    @VisibleForTesting
    public static void checkForTestRunsPastTTL(TestRunDAO testRunDAO, PerformanceReportDAO reportDAO, TestDAO testDAO,
        UserMessenger httpMessenger, UserMessenger slackUserMessenger, String externalUrl, BooleanSupplier isPaused)
    {
        logger.withScopedInfo("Checking for test runs older than three years")
            .run(() -> {
                final Instant lowerBound = Instant.now().minus(THREE_YEARS);
                Map<String, List<TestRun>> testRunsByUser = new HashMap<>();
                testRunDAO.getAll()
                    .takeWhile(ignored -> !isPaused.getAsBoolean())
                    .filter(testRun -> testRunCanBeDeleted(testRun, lowerBound, reportDAO))
                    .forEach(testRun -> testRunsByUser.computeIfAbsent(testRun.getOwner(), k -> new ArrayList<>())
                        .add(testRun));

                testRunsByUser.forEach((owner, testRuns) -> deleteTestRunsAndNotifyOwner(owner, testRuns, testDAO,
                    httpMessenger, slackUserMessenger, externalUrl));
            });
    }

    @Override
    protected void runTask()
    {
        checkForTestRunsPastTTL(this.testRunDAO, this.reportDAO, this.testDAO, this.htmlUserMessenger,
            this.slackUserMessenger, this.externalUrl, this::isPaused);
    }
}
