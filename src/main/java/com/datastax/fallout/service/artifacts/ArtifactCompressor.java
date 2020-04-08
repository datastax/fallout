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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;

import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.FinishedTestRun;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ScopedLogger;

public class ArtifactCompressor extends PeriodicTask
{

    private static final Set<String> INCOMPRESSIBLE_FILE_EXTENSIONS = Set
        .of("gz", "xz", "zip", "hdr", "hgrm", "png", "gif", "jpg");
    private static final ScopedLogger logger = ScopedLogger.getLogger("ArtifactCompressor");
    private Path rootArtifactPath;
    private TestRunDAO testRunDAO;
    private TestDAO testDAO;

    public ArtifactCompressor(boolean startPaused, HashedWheelTimer timer, Duration delay, Duration repeat,
        Path rootArtifactPath, TestRunDAO testRunDAO, TestDAO testDAO)
    {
        super(startPaused, timer, delay, repeat);
        this.rootArtifactPath = rootArtifactPath;
        this.testRunDAO = testRunDAO;
        this.testDAO = testDAO;
    }

    @Override
    protected ScopedLogger logger()
    {
        return logger;
    }

    private static void compressArtifacts(List<Path> artifacts) throws IOException
    {
        for (Path artifactPath : artifacts)
        {
            FileUtils.compressGZIP(artifactPath, artifactPath.resolveSibling(artifactPath.getFileName() + ".gz"));
        }
    }

    @VisibleForTesting
    public static void checkForUncompressedArtifacts(TestRunDAO testRunDAO, Path rootArtifactPath, TestDAO testDAO)
    {
        // gzip all non-compressed artifacts from one week ago
        final Instant oneWeekAgo = Instant.now().minus(7, ChronoUnit.DAYS);
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("Checking for uncompressed artifacts"))
        {
            for (FinishedTestRun finishedTestRun : testRunDAO.getRecentFinishedTestRuns())
            {
                Path artifactPath = Artifacts.buildTestArtifactPath(rootArtifactPath, finishedTestRun);
                List<Path> uncompressedArtifacts = Files
                    .find(artifactPath, 10,
                        (path, attr) -> !attr.isDirectory() &&
                            attr.lastModifiedTime().toInstant().isBefore(oneWeekAgo) &&
                            !INCOMPRESSIBLE_FILE_EXTENSIONS
                                .contains(com.google.common.io.Files.getFileExtension(path.toString())))
                    .collect(Collectors.toList());
                if (!uncompressedArtifacts.isEmpty())
                {
                    TestRun testRun = testRunDAO.getByTestRunId(finishedTestRun.getTestRunId());

                    Test test = testDAO.get(testRun.getOwner(), testRun.getTestName());
                    long testRunSizeBeforeCompression = testRun.getArtifactsSizeBytes().get();

                    logger.info("Compressing artifacts for testrun: {}", testRun.getShortName());
                    compressArtifacts(uncompressedArtifacts);

                    //Update test run to show compressed artifact sizes
                    testRun.updateArtifacts(Artifacts.findTestRunArtifacts(rootArtifactPath, testRun));
                    testRunDAO.update(testRun);

                    //Update total test size on disk
                    testDAO.changeSizeOnDiskBytes(testRun,
                        testRun.getArtifactsSizeBytes().get() - testRunSizeBeforeCompression);
                }
            }
        }
        catch (Exception e)
        {
            logger.error("Error while compressing artifacts", e);
        }
    }

    @Override
    protected void runTask()
    {
        checkForUncompressedArtifacts(this.testRunDAO, this.rootArtifactPath, this.testDAO);
    }
}
