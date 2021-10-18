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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import io.netty.util.HashedWheelTimer;

import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.PeriodicTask;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.util.DateUtils;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.ScopedLogger;

public class ArtifactCompressor extends PeriodicTask
{
    private static final Set<String> INCOMPRESSIBLE_FILE_EXTENSIONS = Set
        .of("gz", "xz", "zip", "png", "gif", "jpg");
    private static final ScopedLogger logger = ScopedLogger.getLogger(ArtifactCompressor.class);
    private Path rootArtifactPath;
    private TestRunDAO testRunDAO;
    private TestDAO testDAO;

    public static final Range<java.time.Duration> COMPRESSION_RANGE_DAYS =
        Range.closedOpen(java.time.Duration.ofDays(1), java.time.Duration.ofDays(14));

    public ArtifactCompressor(boolean startPaused, HashedWheelTimer timer,
        ReentrantLock runningTaskLock, Duration delay, Duration repeat,
        Path rootArtifactPath, TestRunDAO testRunDAO, TestDAO testDAO)
    {
        super(startPaused, timer, runningTaskLock, delay, repeat);
        this.rootArtifactPath = rootArtifactPath;
        this.testRunDAO = testRunDAO;
        this.testDAO = testDAO;
    }

    @Override
    protected ScopedLogger logger()
    {
        return logger;
    }

    void checkAllFinishedTestRunsForUncompressedArtifacts()
    {
        runExclusively(() -> logger
            .withScopedInfo("Checking for uncompressed artifacts in ALL finished testruns older than {} days",
                COMPRESSION_RANGE_DAYS.lowerEndpoint().toDays())
            .run(() -> {
                final Instant lowerBound = Instant.now().minus(COMPRESSION_RANGE_DAYS.lowerEndpoint());

                testRunDAO.getAllEvenIfDeleted()
                    .takeWhile(ignored -> !isPaused())
                    .filter(testRun -> testRun.getState() != null &&
                        testRun.getState().finished() &&
                        testRun.getFinishedAt() != null &&
                        testRun.getFinishedAt().toInstant().isBefore(lowerBound))
                    .forEach(testRun -> checkForUncompressedArtifacts(rootArtifactPath, testDAO, testRun, true));
            }));
    }

    @VisibleForTesting
    public static void checkRecentlyFinishedTestRunsForUncompressedArtifacts(Path rootArtifactPath,
        TestRunDAO testRunDAO, TestDAO testDAO, BooleanSupplier isPaused)
    {
        logger
            .withScopedInfo("Checking for uncompressed artifacts in finished testruns between {} and {} days old",
                COMPRESSION_RANGE_DAYS.lowerEndpoint().toDays(), COMPRESSION_RANGE_DAYS.upperEndpoint().toDays())
            .run(() -> {

                final var now = Instant.now();

                testRunDAO
                    .getAllFinishedTestRunsThatFinishedBetweenInclusive(
                        now.minus(COMPRESSION_RANGE_DAYS.upperEndpoint()),
                        now.minus(COMPRESSION_RANGE_DAYS.lowerEndpoint())
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
                    .forEach(testRun -> checkForUncompressedArtifacts(rootArtifactPath, testDAO, testRun, false));
            });
    }

    private static String testRunInfo(ReadOnlyTestRun testRun)
    {
        return String.format("%s %s %s", testRun.getClass().getSimpleName(),
            DateUtils.formatUTCDate(testRun.getFinishedAt()), testRun.getShortName());
    }

    private static void checkForUncompressedArtifacts(Path rootArtifactPath, TestDAO testDAO, TestRun testRun,
        boolean forceUpdateDB)
    {
        try
        {
            logger.debug("Checking {}", testRunInfo(testRun));

            Path artifactPath = Artifacts.buildTestRunArtifactPath(rootArtifactPath, testRun);
            if (!artifactPath.toFile().exists())
            {
                logger.warn("Artifact path {} for {} does not exist; skipping", artifactPath, testRunInfo(testRun));
                return;
            }
            List<Path> uncompressedArtifacts = Files
                .find(artifactPath, 10,
                    (path, attr) -> !attr.isDirectory() &&
                        !INCOMPRESSIBLE_FILE_EXTENSIONS
                            .contains(com.google.common.io.Files.getFileExtension(path.toString())))
                .collect(Collectors.toList());

            if (uncompressedArtifacts.isEmpty())
            {
                logger.info("No compressible artifacts at {} for {}; {}", artifactPath, testRunInfo(testRun),
                    forceUpdateDB ? "updating the db to match what is on disk" : "skipping");
                if (!forceUpdateDB)
                {
                    return;
                }
            }
            else
            {
                logger.withScopedInfo("Compressing artifacts for {}", testRunInfo(testRun))
                    .run(() -> compressArtifacts(uncompressedArtifacts));
            }

            // Update test run to show compressed artifact sizes

            Map<String, Long> testRunArtifacts = null;
            try
            {
                testRunArtifacts = Artifacts.findTestRunArtifacts(rootArtifactPath, testRun);
            }
            catch (IOException e)
            {
                logger.error(String.format("Unexpected exception fetching compressed artifacts " +
                    "for %s; artifact list and size will now be incorrect!",
                    testRunInfo(testRun)), e);
            }

            if (testRunArtifacts == null)
            {
                return;
            }

            if (testRunArtifacts.isEmpty())
            {
                // Write empty artifacts anyway: this keeps the DB consistent with filesystem reality.
                logger.error("Artifacts have disappeared after compression for {}; " +
                    "updating anyway", testRunInfo(testRun));
            }

            testDAO.updateTestRunArtifacts(testRun.getTestRunIdentifier(), testRunArtifacts)
                .ifPresentOrElse(
                    oldAndNewSizes -> {
                        final var oldSize = oldAndNewSizes.getLeft();
                        final var newSize = oldAndNewSizes.getRight();
                        if (newSize > oldSize)
                        {
                            logger.warn("Compressing {} cost {} bytes (before: {}  after: {})",
                                testRunInfo(testRun), newSize - oldSize, oldSize, newSize);
                        }
                        else
                        {
                            logger.info("Compressing {} saved {} bytes (before: {}  after: {})",
                                testRunInfo(testRun), oldSize - newSize, oldSize, newSize);
                        }
                    },
                    () -> {
                        logger.info("TestRun {} was deleted before we could update artifacts",
                            testRunInfo(testRun));
                    }
                );
        }
        catch (Exception e)
        {
            logger.error("Error while compressing artifacts", e);
        }
    }

    private static void compressArtifacts(List<Path> artifacts)
    {
        for (Path artifactPath : artifacts)
        {
            logger.debug("{}", artifactPath);
            try
            {
                FileUtils.compressGZIP(artifactPath, artifactPath.resolveSibling(artifactPath.getFileName() + ".gz"));
                Files.delete(artifactPath);
            }
            catch (IOException e)
            {
                logger.error(String.format("Unexpected exception while compressing %s", artifactPath), e);
            }
        }
    }

    @Override
    protected void runTask()
    {
        checkRecentlyFinishedTestRunsForUncompressedArtifacts(this.rootArtifactPath, this.testRunDAO, this.testDAO,
            this::isPaused);
    }
}
