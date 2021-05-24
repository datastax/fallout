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
package com.datastax.fallout;

import java.nio.file.Files;
import java.nio.file.Path;

import io.dropwizard.util.Generics;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory.TestRunScratchSpace;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;
import com.datastax.fallout.util.Exceptions;

import static java.nio.file.StandardOpenOption.APPEND;

public class TestHelpers
{
    private final static Logger logger = LoggerFactory.getLogger(TestHelpers.class);

    private volatile static Path testRunArtifactPath = null;

    public static boolean runExpensiveTests()
    {
        return Boolean.getBoolean("runExpensiveTests");
    }

    public static boolean runTestsThatCostMoney()
    {
        return Boolean.getBoolean("runTestsThatCostMoney");
    }

    public static boolean runCCMTests()
    {
        return !"true".equalsIgnoreCase(System.getProperty("skipCCMTests"));
    }

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

    public static void setTestRunArtifactPath(Path testCaseOutputDir)
    {
        TestHelpers.testRunArtifactPath = setupArtifactPath(testCaseOutputDir);
    }

    public static void createArtifact(Path testRunRootArtifactPath, String artifactName, String artifactContent)
    {
        Exceptions.runUnchecked(() -> {
            final var absolutePath = testRunRootArtifactPath.resolve(artifactName);
            Files.createDirectories(absolutePath.getParent());
            Files.writeString(absolutePath, artifactContent);
        });
    }

    /** Makes a {@link TestRunScratchSpace} available for tests via {@link #persistentTestClassScratchSpace()}
     *  and {@link #persistentTestScratchSpace()}.  These scratch spaces are never cleaned up. */
    public static class WithTestRunScratchSpace extends WithPersistentTestOutputDir
    {
        private static TestRunScratchSpaceFactory classFactory;
        private TestRunScratchSpaceFactory testFactory;

        @BeforeAll
        public static void setClassFactory()
        {
            classFactory = new TestRunScratchSpaceFactory(persistentTestClassOutputDir().resolve("scratch"));
        }

        @BeforeEach
        public void setTestFactory()
        {
            testFactory = new TestRunScratchSpaceFactory(persistentTestOutputDir().resolve("scratch"));
        }

        private static TestRunScratchSpace getTestRunScratchSpace(
            TestRunScratchSpaceFactory testRunScratchSpaceFactory)
        {
            return testRunScratchSpaceFactory
                .create(new TestRunIdentifier(Fakes.TEST_USER_EMAIL, Fakes.TEST_NAME, Fakes.TEST_RUN_ID));
        }

        public static TestRunScratchSpace persistentTestClassScratchSpace()
        {
            return getTestRunScratchSpace(classFactory);
        }

        public TestRunScratchSpace persistentTestScratchSpace()
        {
            return getTestRunScratchSpace(testFactory);
        }
    }

    public static abstract class ArtifactTest extends WithTestRunScratchSpace
    {
        @BeforeEach
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
                () -> Files.writeString(absoluteArtifactPath, artifactContent));

            return absoluteArtifactPath;
        }

        public void updateTestRunArtifact(Path absoluteArtifactPath, String artifactContent)
        {
            logger.info("Updating {}", absoluteArtifactPath);
            Exceptions.runUnchecked(
                () -> Files.writeString(absoluteArtifactPath, artifactContent, APPEND));
        }
    }

    public static abstract class FalloutTest<FC extends FalloutConfiguration> extends ArtifactTest
    {
        private FC falloutConfiguration = null;
        private final User testUser = Fakes.makeUser();

        public FC falloutConfiguration()
        {
            return falloutConfiguration;
        }

        public User getTestUser()
        {
            return testUser;
        }

        @BeforeEach
        public void setFalloutConfiguration()
        {
            final var configurationClass =
                Generics.<FC>getTypeParameter(getClass(), FalloutConfiguration.class);
            falloutConfiguration = Exceptions.getUnchecked(() -> configurationClass.getConstructor().newInstance());
            falloutConfiguration.setArtifactPath(testRunArtifactPath.toString());
        }
    }

    public static abstract class FalloutConfigManagerTest<FC extends FalloutConfiguration> extends FalloutTest<FC>
    {
        public NodeGroupBuilder createFakeNodeGroupBuilder(Ensemble.Role role)
        {
            return NodeGroupBuilder.create()
                .withName("fake-node-group")
                .withPropertyGroup(new WritablePropertyGroup())
                .withProvisioner(new FakeProvisioner())
                .withConfigurationManager(new FakeConfigurationManager())
                .withNodeCount(1)
                .withRole(role)
                .withTestRunArtifactPath(testRunArtifactPath());
        }

        public Ensemble createEnsemble(NodeGroupBuilder serverNodeGroupBuilder,
            NodeGroupBuilder observerNodeGroupBuilder)
        {
            return EnsembleBuilder.create()
                .withTestRunId(Fakes.TEST_RUN_ID)
                .withServerGroup(serverNodeGroupBuilder)
                .withClientGroup(serverNodeGroupBuilder)
                .withObserverGroup(observerNodeGroupBuilder)
                .withControllerGroup(createFakeNodeGroupBuilder(Ensemble.Role.CONTROLLER))
                .build(testRunArtifactPath(), persistentTestScratchSpace());
        }

        public Ensemble createEnsemble(NodeGroupBuilder serverNodeGroupBuilder)
        {
            return createEnsemble(serverNodeGroupBuilder, createFakeNodeGroupBuilder(Ensemble.Role.OBSERVER));
        }
    }
}
