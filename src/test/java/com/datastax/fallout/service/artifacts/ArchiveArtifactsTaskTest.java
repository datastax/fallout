/*
 * Copyright 2022 DataStax, Inc.
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

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.PerformanceReport;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.NetworkUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.artifacts.ArtifactCompressorTest.COMPRESSIBLE_ARTIFACT_NAME;
import static com.datastax.fallout.service.artifacts.ArtifactCompressorTest.COMPRESSIBLE_ARTIFACT_WITHIN_DIR_NAME;
import static com.datastax.fallout.service.artifacts.ArtifactCompressorTest.INCOMPRESSIBLE_ARTIFACT_NAME;
import static com.datastax.fallout.service.artifacts.ArtifactCompressorTest.SYMLINK_DIR_NAME;
import static com.datastax.fallout.service.core.Fakes.TEST_NAME;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.core.TestRun.State.PASSED;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;
import static com.datastax.fallout.service.resources.server.ArtifactResourceTest.ARTIFACT_CONTENT;
import static com.datastax.fallout.service.resources.server.ArtifactResourceTest.LARGE_ARTIFACT_CONTENT;

@Tag("requires-db")
public class ArchiveArtifactsTaskTest extends ManagedArtifactServiceTest
{
    private static final String S3_ARCHIVE_BUCKET_VAR = "FALLOUT_ARCHIVE_BUCKET";
    private static final String keyspace = "archive_artifacts_task_test";

    private static final Map<String, String> artifactContentMap = Map.of(
        COMPRESSIBLE_ARTIFACT_NAME, LARGE_ARTIFACT_CONTENT,
        INCOMPRESSIBLE_ARTIFACT_NAME, ARTIFACT_CONTENT,
        COMPRESSIBLE_ARTIFACT_WITHIN_DIR_NAME, ARTIFACT_CONTENT
    );

    private static TestRunDAO testRunDAO;
    private static TestDAO testDAO;
    private static CassandraDriverManager driverManager;
    private static Session session;

    private Test test;
    private TestRun testRun;
    private Path testRunArtifactPath;

    @BeforeAll
    public static void startCassandra() throws Exception
    {
        driverManager = createDriverManager(keyspace);

        testRunDAO = new TestRunDAO(driverManager);
        testDAO = new TestDAO(driverManager, testRunDAO);

        driverManager.start();
        testRunDAO.start();
        testDAO.start();

        session = driverManager.getSession();
        session.execute(String.format("USE %s", keyspace));
    }

    @BeforeEach
    public void setup()
    {
        session.execute("TRUNCATE tests");
        session.execute("TRUNCATE test_runs");
        session.execute("TRUNCATE finished_test_runs");
        testRunDAO.maybeAddFinishedTestRunEndStop(Date.from(Instant.now().minus(20, ChronoUnit.DAYS)));

        makeTest();
        makeTestRunWithArtifacts();
    }

    void makeTest()
    {
        test = Test.createTest(TEST_USER_EMAIL, TEST_NAME, null);
        testDAO.update(test);
    }

    void makeTestRunWithArtifacts()
    {
        testRun = createTestRun(test.getName(), testRunId);
        testRunArtifactPath = Artifacts.buildTestRunArtifactPath(artifactRootPath(), testRun);

        FileUtils.createDirs(testRunArtifactPath);

        final String compressibleArtifact = COMPRESSIBLE_ARTIFACT_NAME;
        final String incompressibleArtifact = INCOMPRESSIBLE_ARTIFACT_NAME;
        final String artifactUnderDir = COMPRESSIBLE_ARTIFACT_WITHIN_DIR_NAME;
        final String symlinkDir = SYMLINK_DIR_NAME;

        assertThat(testRunArtifactPath.resolve(compressibleArtifact)).doesNotExist();
        assertThat(testRunArtifactPath.resolve(incompressibleArtifact)).doesNotExist();
        assertThat(testRunArtifactPath.resolve(artifactUnderDir)).doesNotExist();
        assertThat(testRunArtifactPath.resolve(symlinkDir)).doesNotExist();

        TestHelpers.createArtifact(testRunArtifactPath, compressibleArtifact, LARGE_ARTIFACT_CONTENT);
        TestHelpers.createArtifact(testRunArtifactPath, incompressibleArtifact, ARTIFACT_CONTENT);
        TestHelpers.createArtifact(testRunArtifactPath, artifactUnderDir, ARTIFACT_CONTENT);

        Exceptions.runUncheckedIO(() -> Files.createSymbolicLink(testRunArtifactPath.resolve(symlinkDir),
            testRunArtifactPath.resolve(COMPRESSIBLE_ARTIFACT_WITHIN_DIR_NAME)));

        assertThat(testRunArtifactPath.resolve(compressibleArtifact)).exists();
        assertThat(testRunArtifactPath.resolve(incompressibleArtifact)).exists();
        assertThat(testRunArtifactPath.resolve(artifactUnderDir)).exists();
        assertThat(testRunArtifactPath.resolve(symlinkDir)).exists();

        // Set finished date to have files be candidates for archival
        Instant finishedAt = Instant.now().minus(ArchiveArtifactsTask.ARCHIVE_RANGE_DAYS.lowerEndpoint().plusDays(1));

        testRun.setState(PASSED);
        testRun.setFinishedAt(Date.from(finishedAt));
        testRun.updateArtifacts(Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(testRunArtifactPath)));
        testRunDAO.update(testRun);
    }

    /**
     * Moves artifacts from expected the artifact path to 'hidden' location. Useful for testing everything except the
     * interactions with S3.
     */
    private class LocalArtifactArchive extends ArtifactArchive
    {
        private final Path hiddenArtifactLocaltion;

        LocalArtifactArchive(String rootArtifactLocation)
        {
            super(rootArtifactLocation);
            hiddenArtifactLocaltion = Paths.get(rootArtifactLocation, "hidden-archive");
        }

        @Override
        public URL getArtifactUrl(Path artifact)
        {
            try
            {
                return new URL(String.format("file://%s", hiddenArtifactLocaltion.resolve(artifact)));
            }
            catch (MalformedURLException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void uploadTestRun(TestRun testRun) throws IOException
        {
            File expected = Artifacts.buildTestRunArtifactPath(rootArtifactLocation, testRun).toFile();
            File hidden = Artifacts.buildTestRunArtifactPath(hiddenArtifactLocaltion, testRun).toFile();
            org.apache.commons.io.FileUtils.copyDirectory(expected, hidden);
        }

        @Override
        protected void downloadArtifacts(TestRun testRun) throws IOException
        {
            File expected = Artifacts.buildTestRunArtifactPath(rootArtifactLocation, testRun).toFile();
            File hidden = Artifacts.buildTestRunArtifactPath(hiddenArtifactLocaltion, testRun).toFile();
            org.apache.commons.io.FileUtils.copyDirectory(hidden, expected);
        }

        @Override
        public void uploadReport(PerformanceReport report) throws IOException
        {
        }
    }

    private ArtifactArchive artifactArchive()
    {
        var archiveBucket = System.getenv(S3_ARCHIVE_BUCKET_VAR);
        LoggerFactory.getLogger(ArchiveArtifactsTaskTest.class).info("Property: {}", archiveBucket);
        if (null != archiveBucket && !archiveBucket.isEmpty())
        {
            return new ArtifactArchive.S3ArtifactArchive(artifactRootPath().toString(), archiveBucket);
        }
        return new LocalArtifactArchive(artifactRootPath().toString());
    }

    private void checkArtifactContents(Function<String, String> contentFunc)
    {
        for (var e : artifactContentMap.entrySet())
        {
            assertThat(contentFunc.apply(e.getKey())).isEqualTo(e.getValue());
        }
    }

    private Path relativeTestRunArtifactPath(TestRun testRun, String artifactPath)
    {
        return Paths.get(
            String.format("%s/%s/%s/%s",
                testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(), artifactPath));
    }

    @org.junit.jupiter.api.Test
    public void artifact_archive_task_moves_artifacts_to_readable_and_restorable_archive() throws IOException
    {
        ArchiveArtifactsTask.archiveTestRuns(testRunDAO, artifactArchive(), artifactRootPath(), () -> false);
        checkArtifactContents(
            (artifactPath) -> Exceptions.getUncheckedIO(
                () -> NetworkUtils.downloadUrlAsString(
                    artifactArchive().getArtifactUrl(relativeTestRunArtifactPath(testRun, artifactPath)))));
        artifactArchive().downloadArtifacts(testRun);

        checkArtifactContents(
            (artifactPath) -> Exceptions.getUncheckedIO(
                () -> FileUtils.readString(testRunArtifactPath.resolve(artifactPath))));
    }
}
