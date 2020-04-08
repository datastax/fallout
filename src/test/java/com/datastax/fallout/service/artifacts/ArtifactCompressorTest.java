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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import com.datastax.driver.core.Session;
import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.test.utils.categories.RequiresDb;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.service.core.Fakes.TEST_NAME;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;
import static com.datastax.fallout.service.resources.server.ArtifactResourceTest.ARTIFACT_CONTENT;
import static com.datastax.fallout.service.resources.server.ArtifactResourceTest.LARGE_ARTIFACT_CONTENT;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@Category(RequiresDb.class)
public class ArtifactCompressorTest extends ManagedArtifactServiceTest
{
    private Path testRunArtifactPath;
    private static TestRunDAO testRunDAO;
    private static TestDAO testDAO;
    private static CassandraDriverManager driverManager;
    private static final String keyspace = "artifact_compressor_test";
    private static final String testRunId = "69A38F36-8A91-4ABB-A2C8-B669189FEFD5";

    @BeforeClass
    public static void startCassandra() throws Exception
    {
        driverManager = createDriverManager(keyspace);

        testRunDAO = new TestRunDAO(driverManager);
        testDAO = new TestDAO(driverManager, testRunDAO);

        driverManager.start();
        testRunDAO.start();
        testDAO.start();

        Session session = driverManager.getSession();
        session.execute(String.format("USE %s", keyspace));
        session.execute("TRUNCATE test_runs");
        session.execute("TRUNCATE finished_test_runs");
    }

    @Before
    public void setup() throws IOException
    {
        testRunArtifactPath = artifactPathForTestRun("testName", testRunId);

        FileUtils.deleteDirectory(testRunArtifactPath.toFile());

        Files.createDirectories(testRunArtifactPath);

        testRunDAO.maybeAddFinishedTestRunEndStop(Date.from(Instant.now().minus(10, ChronoUnit.DAYS)));
    }

    @AfterClass
    public static void stopCassandra() throws Exception
    {
        testRunDAO.stop();
        testDAO.stop();
        driverManager.stop();
    }

    @org.junit.Test
    public void artifact_compressor_successfully_compresses_artifacts()
    {
        TestRun testRun;
        final String compressibleArtifact = "monolith.log";
        final String incompressibleArtifact = "the_scream.png";
        final String recentlyModifiedArtifact = "perf_report.csv";

        assertThat(testRunArtifactPath.resolve(compressibleArtifact)).doesNotExist();
        assertThat(testRunArtifactPath.resolve(incompressibleArtifact)).doesNotExist();
        assertThat(testRunArtifactPath.resolve(recentlyModifiedArtifact)).doesNotExist();

        TestHelpers.createArtifact(testRunArtifactPath, compressibleArtifact, LARGE_ARTIFACT_CONTENT);
        TestHelpers.createArtifact(testRunArtifactPath, incompressibleArtifact, ARTIFACT_CONTENT);
        TestHelpers.createArtifact(testRunArtifactPath, recentlyModifiedArtifact, ARTIFACT_CONTENT);

        assertThat(testRunArtifactPath.resolve(compressibleArtifact)).exists();
        assertThat(testRunArtifactPath.resolve(incompressibleArtifact)).exists();
        assertThat(testRunArtifactPath.resolve(recentlyModifiedArtifact)).exists();

        // Set last modified date to have files be candidates for compression
        Instant nineDaysAgo = Instant.now().minus(9, ChronoUnit.DAYS);
        testRunArtifactPath.resolve(compressibleArtifact).toFile().setLastModified(nineDaysAgo.toEpochMilli());
        testRunArtifactPath.resolve(incompressibleArtifact).toFile().setLastModified(nineDaysAgo.toEpochMilli());

        testRun = createTestRun(TEST_NAME, testRunId);
        testRun.setState(TestRun.State.PASSED);
        testRun.setFinishedAt(Date.from(nineDaysAgo));
        testRun.updateArtifacts(Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(testRunArtifactPath)));
        testRunDAO.update(testRun);

        // No definition needed as we're just using this for artifact size tests
        Test test = Test.createTest(TEST_USER_EMAIL, TEST_NAME, null);
        Long uncompressedSize = testRun.getArtifactsSizeBytes().get();
        test.setSizeOnDiskBytes(uncompressedSize);
        testDAO.update(test);

        ArtifactCompressor.checkForUncompressedArtifacts(testRunDAO, artifactRootPath(), testDAO);
        // Check the file system directly because the test run will strip the .gz suffix
        Set<String> artifacts =
            Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(testRunArtifactPath).keySet());

        assertThat(artifacts.size()).isEqualTo(3);
        assertThat(artifacts).contains("monolith.log.gz", "the_scream.png", "perf_report.csv");

        test = testDAO.get(TEST_USER_EMAIL, TEST_NAME);
        assertThat(test.getSizeOnDiskBytes()).isLessThan(uncompressedSize);
    }
}
