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
package com.datastax.fallout.service.artifacts;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.datastax.driver.core.Session;
import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.test.utils.categories.RequiresDb;

import static com.datastax.fallout.service.core.Fakes.TEST_NAME;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;
import static com.datastax.fallout.service.resources.server.ArtifactResourceTest.ARTIFACT_CONTENT;
import static org.assertj.core.api.Assertions.assertThat;

@Category(RequiresDb.class)
public class ArtifactScrubberTest extends ManagedArtifactServiceTest
{
    private Path validTestRunArtifactPath;
    private Path noMatchingTestRunArtifactPath;
    private Path noMatchingTestArtifactPath;
    private static TestRunDAO testRunDAO;
    private static UserDAO userDAO;
    private static Session session;
    private static CassandraDriverManager driverManager;
    private static final String keyspace = "artifact_scrubber_test";
    static final String testRunId = "69A38F36-8A91-4ABB-A2C8-B669189FEFD5";

    @BeforeClass
    public static void startCassandra() throws Exception
    {
        driverManager = createDriverManager(keyspace);

        testRunDAO = new TestRunDAO(driverManager);
        userDAO = new UserDAO(driverManager, new SecurityUtil(), Optional.empty());

        driverManager.start();
        testRunDAO.start();
        userDAO.start();

        session = driverManager.getSession();
        session.execute(String.format("USE %s", keyspace));
        session.execute("TRUNCATE users");
        session.execute("TRUNCATE test_runs");
    }

    @Before
    public void setup() throws IOException
    {
        validTestRunArtifactPath = artifactPathForTestRun("testName", testRunId);
        noMatchingTestRunArtifactPath = artifactPathForTestRun("testName", "b8ec39dc-74b3-439e-a6a7-c8323fa00e0f");
        noMatchingTestArtifactPath = artifactPathForTestRun("noMatchTestName", "85e25506-0a79-4d8a-ad53-85757653025a");

        FileUtils.deleteDirectory(validTestRunArtifactPath.toFile());
        FileUtils.deleteDirectory(noMatchingTestRunArtifactPath.toFile());
        FileUtils.deleteDirectory(noMatchingTestArtifactPath.toFile());

        Files.createDirectories(validTestRunArtifactPath);
        Files.createDirectories(noMatchingTestRunArtifactPath);
        Files.createDirectories(noMatchingTestArtifactPath);
    }

    @AfterClass
    public static void stopCassandra() throws Exception
    {
        testRunDAO.stop();
        userDAO.stop();
        driverManager.stop();
    }

    @Test
    public void artifact_scrubber_successfully_deletes_artifacts()
    {
        TestRun testRun;
        final String artifactName = "monolith.log";
        final String badArtifactName = "goliath.log";

        assertThat(validTestRunArtifactPath.resolve(artifactName)).doesNotExist();
        assertThat(noMatchingTestRunArtifactPath.resolve(badArtifactName)).doesNotExist();
        assertThat(noMatchingTestArtifactPath.resolve(badArtifactName)).doesNotExist();

        TestHelpers.createArtifact(validTestRunArtifactPath, artifactName, ARTIFACT_CONTENT);
        TestHelpers.createArtifact(noMatchingTestRunArtifactPath, badArtifactName, ARTIFACT_CONTENT);
        TestHelpers.createArtifact(noMatchingTestArtifactPath, badArtifactName, ARTIFACT_CONTENT);

        userDAO.createUserIfNotExists("owner", TEST_USER_EMAIL, "");
        testRun = createTestRun(TEST_NAME, testRunId);
        testRunDAO.update(testRun);

        assertThat(validTestRunArtifactPath.resolve(artifactName)).exists();
        assertThat(noMatchingTestRunArtifactPath.resolve(badArtifactName)).exists();
        assertThat(noMatchingTestArtifactPath.resolve(badArtifactName)).exists();

        ArtifactScrubber artifactScrubber = new ArtifactScrubber(true, null, null, null, artifactRootPath(), testRunDAO,
            userDAO);
        artifactScrubber.checkForOrphanedArtifacts();

        assertThat(validTestRunArtifactPath.resolve(artifactName)).exists();
        assertThat(noMatchingTestRunArtifactPath.resolve(badArtifactName)).doesNotExist();
        assertThat(noMatchingTestArtifactPath.resolve(badArtifactName)).doesNotExist();
    }
}
