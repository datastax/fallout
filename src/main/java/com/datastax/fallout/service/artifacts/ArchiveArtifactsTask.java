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

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import io.netty.util.HashedWheelTimer;

import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.PeriodicTask;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ScopedLogger;

import static com.datastax.fallout.service.artifacts.ArtifactCompressor.testRunInfo;
import static com.datastax.fallout.service.artifacts.ArtifactScrubber.MAX_TRIES_TO_DELETE_DIRECTORY;
import static com.datastax.fallout.service.artifacts.ArtifactScrubber.tryToDeleteDirectory;

public class ArchiveArtifactsTask extends PeriodicTask
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(ArchiveArtifactsTask.class);

    /**
     * Artifacts should be archived 15 days after the test run finishes. Two week grace period allows for errors during
     * archiving to be fixed and deployed, as well as some margin of error for scheduling.
     *
     * Third stage of artifact lifecycle, see second stage {@link ArtifactCompressor#COMPRESSION_RANGE_DAYS}.
     */
    public static final Range<java.time.Duration> ARCHIVE_RANGE_DAYS =
        Range.closedOpen(java.time.Duration.ofDays(15), java.time.Duration.ofDays(31));

    private final ArtifactArchive artifactArchive;
    private final TestRunDAO testRunDAO;
    private final Path rootArtifactPath;

    public ArchiveArtifactsTask(boolean startPaused, HashedWheelTimer timer, ReentrantLock runningTaskLock,
        Duration delay, Duration repeat, ArtifactArchive artifactArchive, TestRunDAO testRunDAO, Path rootArtifactPath)
    {
        super(startPaused, timer, runningTaskLock, delay, repeat);
        this.artifactArchive = artifactArchive;
        this.testRunDAO = testRunDAO;
        this.rootArtifactPath = rootArtifactPath;
    }

    @Override
    protected ScopedLogger logger()
    {
        return logger;
    }

    @VisibleForTesting
    public static void archiveTestRuns(TestRunDAO testRunDAO, ArtifactArchive artifactArchive, Path rootArtifactPath,
        BooleanSupplier isPaused)
    {
        logger
            .withScopedInfo("Archiving artifacts in finished test runs between {} and {} days old",
                ARCHIVE_RANGE_DAYS.lowerEndpoint().toDays(), ARCHIVE_RANGE_DAYS.upperEndpoint().toDays())
            .run(() -> {
                final var now = Instant.now();
                testRunDAO
                    .getAllFinishedTestRunsThatFinishedBetweenInclusive(
                        now.minus(ARCHIVE_RANGE_DAYS.upperEndpoint()),
                        now.minus(ARCHIVE_RANGE_DAYS.lowerEndpoint())
                    )
                    .takeWhile(ignored -> !isPaused.getAsBoolean())
                    .flatMap(finishedTestRun -> {
                        final var testRun = testRunDAO.getEvenIfDeleted(finishedTestRun.getTestRunIdentifier());
                        if (testRun == null)
                        {
                            logger.error("No corresponding TestRun found for {}; skipping",
                                testRunInfo(finishedTestRun));
                        }
                        return Optional.ofNullable(testRun).stream();
                    })
                    .forEach(testRun -> {
                        try
                        {
                            logger.info("Archiving test run: {}...", testRun.getShortName());
                            artifactArchive.uploadTestRun(testRun);
                            logger.info("Archived test run: {}. Removing from disk...", testRun.getShortName());
                            tryToDeleteDirectory(logger, Artifacts.buildTestRunArtifactPath(rootArtifactPath, testRun),
                                MAX_TRIES_TO_DELETE_DIRECTORY);
                            logger.info("Test run successfully moved to archive: {}", testRun.getShortName());
                        }
                        catch (IOException e)
                        {
                            logger.error(
                                String.format("IOException during archival of test run %s", testRun.getShortName()), e);
                        }
                    });
            });
    }

    @Override
    protected void runTask()
    {
        archiveTestRuns(testRunDAO, artifactArchive, rootArtifactPath, this::isPaused);
    }
}
