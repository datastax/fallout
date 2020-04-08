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
package com.datastax.fallout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;
import com.datastax.fallout.util.Exceptions;

import static java.nio.file.StandardOpenOption.APPEND;

public class TestHelpers
{
    private final static Logger logger = LoggerFactory.getLogger(TestHelpers.class);

    private volatile static Path testRunArtifactPath = null;
    private volatile static FalloutConfiguration fakeFalloutConfiguration = null;

    public static Path setupArtifactPath(Path testCaseOutputDir)
    {
        final Path artifactPath = testCaseOutputDir.resolve("artifacts");
        logger.info("artifact path: {}", artifactPath);

        System.setProperty(Provisioner.FORCE_ARTIFACTS_DIR,
            testCaseOutputDir.resolve("_provisioner_artifacts").toAbsolutePath().toString());
        System.setProperty(Provisioner.FORCE_SCRATCH_DIR,
            testCaseOutputDir.resolve("_provisioner_scratch").toAbsolutePath().toString());
        System.setProperty(Provisioner.FORCE_LIBRARY_DIR,
            testCaseOutputDir.resolve("_provisioner_library").toAbsolutePath().toString());

        return artifactPath;
    }

    private static void setTestRunArtifactPath(Path testCaseOutputDir)
    {
        TestHelpers.testRunArtifactPath = setupArtifactPath(testCaseOutputDir);
    }

    public static void createArtifact(Path testRunRootArtifactPath, String artifactName, String artifactContent)
    {
        Exceptions.runUnchecked(() -> Files.write(testRunRootArtifactPath.resolve(artifactName),
            artifactContent.getBytes(StandardCharsets.UTF_8)));
    }

    protected static class FakeFalloutConfiguration extends FalloutConfiguration
    {
        @Override
        public String getArtifactPath()
        {
            return testRunArtifactPath.toString();
        }
    }

    public static void setFakeFalloutConfiguration()
    {
        fakeFalloutConfiguration = new FakeFalloutConfiguration();
    }

    public static abstract class ArtifactTest extends WithPersistentTestOutputDir
    {
        @Before
        public void setTestRunArtifactPath()
        {
            TestHelpers.setTestRunArtifactPath(persistentTestOutputDir());
        }

        public Path testRunArtifactPath()
        {
            return TestHelpers.testRunArtifactPath;
        }

        public Path createTestRunArtifact(String relativeArtifactPath, String artifactContent)
        {
            logger.info("Creating {}", relativeArtifactPath);
            Path absoluteArtifactPath = testRunArtifactPath().resolve(relativeArtifactPath);
            absoluteArtifactPath.getParent().toFile().mkdirs();

            Exceptions.runUnchecked(
                () -> Files.write(absoluteArtifactPath, artifactContent.getBytes(StandardCharsets.UTF_8)));

            return absoluteArtifactPath;
        }

        public void updateTestRunArtifact(Path absoluteArtifactPath, String artifactContent)
        {
            logger.info("Updating {}", absoluteArtifactPath);
            Exceptions.runUnchecked(
                () -> Files.write(absoluteArtifactPath, artifactContent.getBytes(StandardCharsets.UTF_8), APPEND));
        }
    }

    public static abstract class FalloutTest extends ArtifactTest
    {
        private User testUser = Fakes.makeUser();

        public FalloutConfiguration falloutConfiguration()
        {
            return TestHelpers.fakeFalloutConfiguration;
        }

        public User getTestUser()
        {
            return testUser;
        }

        @Before
        public void setFakeFalloutConfiguration()
        {
            TestHelpers.setFakeFalloutConfiguration();
        }
    }

}
