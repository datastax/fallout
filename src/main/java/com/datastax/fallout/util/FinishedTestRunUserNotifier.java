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
package com.datastax.fallout.util;

import javax.ws.rs.core.UriBuilder;

import java.io.StringWriter;
import java.net.URI;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.core.TestCompletionNotification;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.service.resources.server.TestResource.uriForShowTestRunArtifacts;

public class FinishedTestRunUserNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(FinishedTestRunUserNotifier.class);
    private final String externalUrl;
    private final UserMessenger htmlMailUserMessenger;
    private final UserMessenger slackUserMessenger;

    public FinishedTestRunUserNotifier(String externalUrl, UserMessenger htmlMailUserMessenger,
        UserMessenger slackUserMessenger)
    {
        this.externalUrl = externalUrl;
        this.htmlMailUserMessenger = htmlMailUserMessenger;
        this.slackUserMessenger = slackUserMessenger;
    }

    static class TestRunNotificationDetails
    {
        final TestRun testRun;
        final String testRunUri;

        private TestRunNotificationDetails(TestRun testRun, String testRunUri)
        {
            this.testRun = testRun;
            this.testRunUri = testRunUri;
        }
    }

    private static String notifyBody(String externalUrl, TestRun testRun, String mustacheFilename)
    {
        final MustacheFactory mf = new DefaultMustacheFactory();
        String testUri = UriBuilder.fromUri(URI.create(externalUrl))
            .uri(uriForShowTestRunArtifacts(testRun))
            .toString();

        final StringWriter stringWriter = new StringWriter();

        Mustache notifyBodyTemplate = mf.compile(mf.getReader(mustacheFilename), "usernotifier");
        notifyBodyTemplate.execute(stringWriter, new TestRunNotificationDetails(testRun, testUri));
        return stringWriter.toString();
    }

    private void maybeSendMessage(TestCompletionNotification notificationPref, TestRun testRun,
        UserMessenger messenger, String subject, String body)
    {
        if (notificationPref == TestCompletionNotification.ALL ||
            (notificationPref == TestCompletionNotification.FAILURES && testRun.getState() != TestRun.State.PASSED))
        {
            try
            {
                messenger.sendMessage(testRun.getOwner(), subject, body);
            }
            catch (UserMessenger.MessengerException e)
            {
                logger.error(String.format("TestRun %s: %s failed to notify user", testRun.getTestRunId(),
                    messenger.getClass().toString()), e);
            }
        }
    }

    public void notify(TestRun testRun)
    {
        maybeSendMessage(testRun.getEmailPref(), testRun,
            htmlMailUserMessenger,
            "Fallout test: " + testRun.getTestName() + " " + testRun.getState(),
            notifyBody(externalUrl, testRun, "usernotifier.email.mustache")
        );

        maybeSendMessage(testRun.getSlackPref(), testRun,
            slackUserMessenger,
            null,
            notifyBody(externalUrl, testRun, "usernotifier.slack.mustache")
        );
    }
}
