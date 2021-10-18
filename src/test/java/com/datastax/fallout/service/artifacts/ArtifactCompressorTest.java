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
package com.datastax.fallout.service.artifacts;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import com.datastax.driver.core.Session;
import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.core.Fakes.TEST_NAME;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.core.TestRun.State.PASSED;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;
import static com.datastax.fallout.service.resources.server.ArtifactResourceTest.ARTIFACT_CONTENT;
import static com.datastax.fallout.service.resources.server.ArtifactResourceTest.LARGE_ARTIFACT_CONTENT;

@Tag("requires-db")
public class ArtifactCompressorTest extends ManagedArtifactServiceTest
{
    public static final String COMPRESSIBLE_ARTIFACT_NAME = "monolith.log";
    public static final String INCOMPRESSIBLE_ARTIFACT_NAME = "the_scream.png";

    private Test test;
    private TestRun testRun;
    private static final long INITIAL_TEST_SIZE = 1000;
    private Path testRunArtifactPath;
    private static TestRunDAO testRunDAO;
    private static TestDAO testDAO;
    private static CassandraDriverManager driverManager;

    private static final String keyspace = "artifact_compressor_test";
    private static final String testRunId = "69A38F36-8A91-4ABB-A2C8-B669189FEFD5";
    private static Session session;

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
        testRunDAO.maybeAddFinishedTestRunEndStop(Date.from(Instant.now().minus(10, ChronoUnit.DAYS)));

        makeTestWithInitialSize();
        makeTestRunWithArtifacts();
    }

    @AfterAll
    public static void stopCassandra() throws Exception
    {
        testRunDAO.stop();
        testDAO.stop();
        driverManager.stop();
    }

    void makeTestWithInitialSize()
    {
        // No definition needed as we're just using this for artifact size tests
        test = Test.createTest(TEST_USER_EMAIL, TEST_NAME, null);
        test.setSizeOnDiskBytes(INITIAL_TEST_SIZE);
        testDAO.update(test);
    }

    void makeTestRunWithArtifacts()
    {
        testRun = createTestRun(test.getName(), testRunId);
        testRunArtifactPath = Artifacts.buildTestRunArtifactPath(artifactRootPath(), testRun);

        FileUtils.createDirs(testRunArtifactPath);

        final String compressibleArtifact = COMPRESSIBLE_ARTIFACT_NAME;
        final String incompressibleArtifact = INCOMPRESSIBLE_ARTIFACT_NAME;

        assertThat(testRunArtifactPath.resolve(compressibleArtifact)).doesNotExist();
        assertThat(testRunArtifactPath.resolve(incompressibleArtifact)).doesNotExist();

        TestHelpers.createArtifact(testRunArtifactPath, compressibleArtifact, LARGE_ARTIFACT_CONTENT);
        TestHelpers.createArtifact(testRunArtifactPath, incompressibleArtifact, ARTIFACT_CONTENT);

        assertThat(testRunArtifactPath.resolve(compressibleArtifact)).exists();
        assertThat(testRunArtifactPath.resolve(incompressibleArtifact)).exists();

        // Set finished date to have files be candidates for compression
        Instant finishedAt = Instant.now().minus(ArtifactCompressor.COMPRESSION_RANGE_DAYS.lowerEndpoint().plusDays(1));

        testRun.setState(PASSED);
        testRun.setFinishedAt(Date.from(finishedAt));
        testRun.updateArtifacts(Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(testRunArtifactPath)));
        testRunDAO.update(testRun);
        testDAO.increaseSizeOnDiskBytesByTestRunSize(testRun);

        assertThat(testRun.getArtifactsSizeBytes())
            .hasValueSatisfying(size -> assertThat(size).isGreaterThan(0));
        assertThat(testDAO.get(test.getOwner(), test.getName()))
            .hasSizeOnDiskBytes(testRun.getArtifactsSizeBytes().get() + INITIAL_TEST_SIZE);
    }

    long compressAndCheckArtifacts()
    {
        ArtifactCompressor
            .checkRecentlyFinishedTestRunsForUncompressedArtifacts(artifactRootPath(), testRunDAO, testDAO,
                () -> false);

        // Get the artifacts directly because the list in the testrun strips gz suffixes
        final var artifacts =
            Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(testRunArtifactPath));

        assertThat(artifacts).containsOnlyKeys(COMPRESSIBLE_ARTIFACT_NAME + ".gz", INCOMPRESSIBLE_ARTIFACT_NAME);

        final var compressedSize = artifacts.values().stream().mapToLong(Long::longValue).sum();
        assertThat(compressedSize).isGreaterThan(0);

        return compressedSize;
    }

    void checkTestRunArtifacts(long compressedSize, TestRun testRun)
    {
        assertThat(testRun)
            .hasArtifactsSizeBytes(Optional.of(compressedSize));
        assertThat(testRun.getArtifacts())
            .containsOnlyKeys(COMPRESSIBLE_ARTIFACT_NAME, INCOMPRESSIBLE_ARTIFACT_NAME);
    }

    @org.junit.jupiter.api.Test
    public void compresses_testruns_and_updates_testrun_and_test_size()
    {
        final var compressedSize = compressAndCheckArtifacts();

        checkTestRunArtifacts(compressedSize, testRunDAO.get(testRun.getTestRunIdentifier()));

        assertThat(testDAO.get(test.getOwner(), test.getName()))
            .hasSizeOnDiskBytes(compressedSize + INITIAL_TEST_SIZE);
    }

    @org.junit.jupiter.api.Test
    public void compresses_deleted_testruns_and_updates_only_testrun_size_when_test_is_not_deleted()
    {
        testDAO.deleteTestRun(testRun);
        assertThat(testRunDAO.get(testRun.getTestRunIdentifier())).isNull();

        final var compressedSize = compressAndCheckArtifacts();

        checkTestRunArtifacts(compressedSize, testRunDAO.getDeleted(testRun.getTestRunIdentifier()));

        assertThat(testDAO.get(test.getOwner(), test.getName()))
            .hasSizeOnDiskBytes(INITIAL_TEST_SIZE);
    }

    @org.junit.jupiter.api.Test
    public void compresses_deleted_testruns_and_updates_only_testrun_size_when_test_is_deleted()
    {
        testDAO.deleteTestAndTestRuns(test);

        assertThat(testDAO.get(test.getOwner(), test.getName())).isNull();
        assertThat(testRunDAO.get(testRun.getTestRunIdentifier())).isNull();

        final var compressedSize = compressAndCheckArtifacts();

        checkTestRunArtifacts(compressedSize, testRunDAO.getDeleted(testRun.getTestRunIdentifier()));

        assertThat(testDAO.getDeleted(test.getOwner(), test.getName()))
            .hasSizeOnDiskBytes(INITIAL_TEST_SIZE);
    }

    @org.junit.jupiter.api.Test
    public void restoring_a_test_run_that_was_compressed_updates_the_test_size()
    {
        testDAO.deleteTestRun(testRun);

        final var compressedSize = compressAndCheckArtifacts();

        final var dbTestRun = testRunDAO.getDeleted(testRun.getTestRunIdentifier());
        checkTestRunArtifacts(compressedSize, dbTestRun);

        assertThat(testDAO.get(test.getOwner(), test.getName()))
            .hasSizeOnDiskBytes(INITIAL_TEST_SIZE);

        testDAO.restoreTestRun(dbTestRun);

        assertThat(testDAO.get(test.getOwner(), test.getName()))
            .hasSizeOnDiskBytes(compressedSize + INITIAL_TEST_SIZE);
    }
}
