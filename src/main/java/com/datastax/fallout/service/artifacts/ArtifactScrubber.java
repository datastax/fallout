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

import java.nio.file.Path;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import org.apache.commons.io.FileUtils;

import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.ScopedLogger;

public class ArtifactScrubber extends PeriodicTask
{
    private static ScopedLogger logger = ScopedLogger.getLogger("ArtifactScrubber");
    private TestRunDAO testRunDAO;
    private Path rootArtifactPath;
    private UserDAO userDAO;

    public ArtifactScrubber(boolean startPaused, HashedWheelTimer timer, Duration delay, Duration repeat,
        Path rootArtifactPath, TestRunDAO testRunDAO, UserDAO userDAO)
    {
        super(startPaused, timer, delay, repeat);
        this.rootArtifactPath = rootArtifactPath;
        this.testRunDAO = testRunDAO;
        this.userDAO = userDAO;
    }

    @Override
    protected ScopedLogger logger()
    {
        return logger;
    }

    private void deleteOrphanedTestRunArtifacts(String email, String test, String testrunid)
    {
        final Path testrunidPath = rootArtifactPath.resolve(email).resolve(test).resolve(testrunid);
        if (testrunidPath.toFile().isDirectory() && testRunDAO.get(email, test, UUID.fromString(testrunid)) == null)
        {
            try
            {
                if (testRunDAO.getDeleted(email, test, UUID.fromString(testrunid)) == null)
                {
                    logger.info("Found artifacts for test with owner: {}  test name: {}  testrunid: {}  " +
                        "but no matching database entry. Removing artifacts from disk ", email, test, testrunid);
                    FileUtils.deleteDirectory(testrunidPath.toFile());
                }
            }
            catch (Exception e)
            {
                logger.error("Error while deleting orphaned artifacts", e);
            }
        }

    }

    private void deleteOrphanedTestArtifacts(String email, String test)
    {
        final Path testPath = rootArtifactPath.resolve(email).resolve(test);

        if (testPath.toFile().isDirectory())
        {
            String[] testFilePath = testPath.toFile().list();

            if (testFilePath == null)
            {
                return;
            }

            for (String testrunid : testFilePath)
            {
                deleteOrphanedTestRunArtifacts(email, test, testrunid);
            }
        }
    }

    @VisibleForTesting
    public void checkForOrphanedArtifacts()
    {
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("Checking for orphaned artifacts"))
        {
            // Loop through emails, but does not account for missing users
            for (String email : userDAO.getAllEmails())
            {
                final Path emailPath = rootArtifactPath.resolve(email);
                String[] emailFilePath = emailPath.toFile().list();

                if (emailFilePath == null)
                {
                    continue;
                }

                for (String test : emailFilePath)
                {
                    deleteOrphanedTestArtifacts(email, test);
                }
            }
        }
    }

    @Override
    protected void runTask()
    {
        checkForOrphanedArtifacts();
    }
}
