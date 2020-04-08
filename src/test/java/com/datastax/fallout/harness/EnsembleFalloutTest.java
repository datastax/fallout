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
package com.datastax.fallout.harness;

import java.io.IOError;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import org.apache.commons.io.FilenameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.JobFileLoggers;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.commands.Java8LocalCommandExecutor;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.ops.NodeGroupAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public abstract class EnsembleFalloutTest extends TestHelpers.FalloutTest
{
    private final static Logger logger = LoggerFactory.getLogger(EnsembleFalloutTest.class);

    private final static TestRunAbortedStatusUpdater testRunStatusUpdater =
        new TestRunAbortedStatusUpdater(new InMemoryTestRunStateStorage(TestRun.State.CREATED));

    @BeforeClass
    public static void initClojure()
    {
        JepsenApi.preload();
    }

    public static String readYamlFile(String path)
    {
        if (!path.startsWith("/"))
        {
            path = "/" + path;
        }
        try
        {
            logger.info("Loading YAML from " + path);
            URL url = EnsembleFalloutTest.class.getResource(path);
            return Resources.toString(url, Charsets.UTF_8);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    public ActiveTestRunBuilder createActiveTestRunBuilder()
    {
        JobFileLoggers loggers = new JobFileLoggers(testRunArtifactPath(), true);
        return ActiveTestRunBuilder.create()
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunName(currentTestShortName())
            .withLoggers(loggers)
            .withFalloutConfiguration(falloutConfiguration())
            .withTestRunStatusUpdater(testRunStatusUpdater)
            .withUserCredentials(new UserCredentials(getTestUser()))
            .withCommandExecutor(new Java8LocalCommandExecutor())
            .destroyEnsembleAfterTest(true);
    }

    private Path toFullArtifactPath(String relativeOrAbsoluteArtifactPath)
    {
        return testRunArtifactPath().resolve(relativeOrAbsoluteArtifactPath);
    }

    private <Result> Result withArtifactLines(Path fullArtifactPath, Function<Stream<String>, Result> function)
    {
        try (Stream<String> lines = Files.lines(fullArtifactPath))
        {
            return function.apply(lines);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    protected void assertArtifactExists(String artifactPath)
    {
        assertArtifactExists(artifactPath, false);
    }

    protected void assertEmptyArtifactExists(String artifactPath)
    {
        assertArtifactExists(artifactPath, true);
    }

    protected void assertArtifactNotExists(String artifactPath)
    {
        Path path = toFullArtifactPath(artifactPath);
        assertFalse("Path does exists: " + path, path.toFile().exists());
    }

    private void assertArtifactExists(String artifactPath, boolean mustBeEmpty)
    {
        Path path = toFullArtifactPath(artifactPath);
        assertTrue("Path does not exist: " + path, path.toFile().exists());

        Set<String> stringContentFileExts = Sets.newHashSet("txt", "log", "hdr", "csv", "html", "json", "yaml", "svg");
        String fileExtension = FilenameUtils.getExtension(artifactPath).toLowerCase();
        boolean hasStringContent = stringContentFileExts.contains(fileExtension);
        if (hasStringContent)
        {
            List<String> lines = withArtifactLines(path, lines_ -> lines_.limit(5).collect(Collectors.toList()));
            if (mustBeEmpty)
            {
                String error = "Artifact was not empty: " + path + " first 5 lines:\n" + String.join("\n", lines);
                assertTrue(error, lines.isEmpty());
            }
            else
            {
                assertFalse("Artifact was empty: " + path, lines.isEmpty());
            }
        }
        else
        {
            try
            {
                long size = Files.size(path);
                if (mustBeEmpty)
                {
                    assertEquals("Artifact was not empty: " + path, 0, size);
                }
                else
                {
                    assertNotEquals("Artifact was empty: " + path, 0, size);
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
    }

    protected ActiveTestRun createActiveTestRun(String yaml)
    {
        return createActiveTestRun(yaml, Collections.emptyMap());
    }

    private ActiveTestRun createActiveTestRun(String yaml, Map<String, Object> templateParams)
    {
        String expandedYaml = TestDefinition.expandTemplate(yaml, templateParams);
        ActiveTestRunBuilder testRunBuilder = createActiveTestRunBuilder()
            .withEnsembleFromYaml(expandedYaml)
            .withWorkloadFromYaml(expandedYaml);

        ActiveTestRun activeTestRun = testRunBuilder.build();

        Assert.assertNotNull(activeTestRun.getEnsemble());
        for (NodeGroup nodeGroup : activeTestRun.getEnsemble().getUniqueNodeGroupInstances())
        {
            assertEquals(NodeGroup.State.UNKNOWN, nodeGroup.getState());
        }
        return activeTestRun;
    }

    protected TestResult runYamlFile(String yamlPath)
    {
        return runYaml(readYamlFile(yamlPath), Collections.emptyMap());
    }

    private TestResult runYaml(String yamlContent, Map<String, Object> templateParams)
    {
        ActiveTestRun activeTestRun = createActiveTestRun(yamlContent, templateParams);
        TestResult result = performTestRun(activeTestRun);
        return result;
    }

    protected TestResult performTestRunWithMockedComponents(String testYaml,
        Consumer<TestRunnerTestHelpers.MockingComponentFactory> mockingComponentFactoryConsumer)
    {
        final TestRunnerTestHelpers.MockingComponentFactory mockingComponentFactory =
            new TestRunnerTestHelpers.MockingComponentFactory();
        mockingComponentFactoryConsumer.accept(mockingComponentFactory);

        final String testDefinition = getTestClassResource(testYaml);
        final ActiveTestRun activeTestRun = createActiveTestRunBuilder()
            .withWorkloadFromYaml(testDefinition)
            .withEnsembleFromYaml(testDefinition)
            .withComponentFactory(mockingComponentFactory)
            .build();
        return performTestRun(activeTestRun);
    }

    protected TestResult performTestRun(ActiveTestRun activeTestRun)
    {
        activeTestRun.run(logger::error);

        Optional<TestResult> result_ = activeTestRun.getResult();
        assertThat(result_).isPresent();
        TestResult result = result_.get();

        checkFinalEnsembleState(activeTestRun.getEnsemble());

        assertArtifactExists("controller/jepsen-history.json");
        assertArtifactExists("fallout-shared.log");
        if (result.isValid())
        {
            assertEmptyArtifactExists("fallout-errors.log");
        }
        else
        {
            assertArtifactExists("fallout-errors.log");
        }
        return result;
    }

    protected void checkFinalEnsembleState(Ensemble ensemble)
    {
        assertThat(ensemble.getUniqueNodeGroupInstances())
            .allSatisfy(nodeGroup -> assertThat(nodeGroup).hasState(NodeGroup.State.DESTROYED));
    }

    protected boolean maybeAssertYamlFileRunsAndPasses(String yamlPath)
    {
        return maybeAssertYamlFileRunsAndPasses(yamlPath, Collections.emptyMap());
    }

    protected boolean maybeAssertYamlFileRunsAndPasses(String yamlPath, Map<String, Object> templateParams)
    {
        return assertYamlFileRunsWithResult(yamlPath, templateParams, true);
    }

    protected boolean maybeAssertYamlFileRunsAndFails(String yamlPath)
    {
        return assertYamlFileRunsWithResult(yamlPath, Collections.emptyMap(), false);
    }

    private boolean assertYamlFileRunsWithResult(
        String yamlPath, Map<String, Object> templateParams, boolean expectedRunResult)
    {
        String yamlContent = readYamlFile(yamlPath);
        assertYamlRunsWithResult(yamlContent, templateParams, expectedRunResult);
        return true;
    }

    protected void assertYamlRunsWithResult(
        String yamlContent, Map<String, Object> templateParams, boolean expectedRunResult)
    {
        boolean runResult = runYaml(yamlContent, templateParams).isValid();
        assertEquals(expectedRunResult, runResult);
    }

    protected void assertYamlFileValidates(String yamlPath)
    {
        assertYamlValidates(readYamlFile(yamlPath));
    }

    private void assertYamlValidates(String yamlContent)
    {
        Assert.assertNotNull(createActiveTestRun(yamlContent));
    }
}
