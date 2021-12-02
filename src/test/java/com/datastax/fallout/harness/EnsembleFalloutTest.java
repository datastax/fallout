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
package com.datastax.fallout.harness;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.harness.ActiveTestRunBuilder.DeprecatedPropertiesHandling;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.JobFileLoggers;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.commands.Java8LocalCommandExecutor;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ResourceUtils;
import com.datastax.fallout.util.component_discovery.MockingComponentFactory;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public abstract class EnsembleFalloutTest<FC extends FalloutConfiguration> extends TestHelpers.FalloutTest<FC>
{
    private final static Logger logger = LoggerFactory.getLogger(EnsembleFalloutTest.class);

    protected enum YamlFileSource
    {
        /** Read yaml files using {@link com.datastax.fallout.test.utils.WithTestResources#getTestClassResource(String)} */
        USE_TEST_CLASS_RESOURCES,

        /** Read yaml files using <code>EnsembleFalloutTest.class.getResource</code> */
        SHARED
    }

    private final YamlFileSource yamlFileSource;

    protected EnsembleFalloutTest(YamlFileSource yamlFileSource)
    {
        this.yamlFileSource = yamlFileSource;
    }

    protected EnsembleFalloutTest()
    {
        this(YamlFileSource.SHARED);
    }

    @BeforeAll
    public static void initClojure()
    {
        JepsenApi.preload();
    }

    public static String readSharedYamlFile(String path)
    {
        if (!path.startsWith("/"))
        {
            path = "/" + path;
        }
        logger.info("Loading YAML from " + path);
        return ResourceUtils.getResourceAsString(EnsembleFalloutTest.class, path);
    }

    public String readYamlFile(String path)
    {
        return yamlFileSource == YamlFileSource.SHARED ?
            readSharedYamlFile(path) :
            getTestClassResource(path);
    }

    public String expandYamlFile(String path, Map<String, Object> templateParams)
    {
        return TestDefinition.expandTemplate(readYamlFile(path), templateParams);
    }

    public String expandYamlFile(String path)
    {
        return expandYamlFile(path, Map.of());
    }

    public ActiveTestRunBuilder createActiveTestRunBuilder()
    {
        User testUser = getTestUser();
        UserCredentials userCredentials = new UserCredentials(testUser, Optional.empty());

        // secret is needed for example yamls to be valid
        testUser.addGenericSecret("MY_ASTRA_TOKEN", "dummy_astra_token");

        JobFileLoggers loggers = new JobFileLoggers(testRunArtifactPath(), true, userCredentials);
        return ActiveTestRunBuilder.create()
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunIdentifier(Fakes.TEST_RUN_IDENTIFIER)
            .withLoggers(loggers)
            .withFalloutConfiguration(falloutConfiguration())
            .withTestRunStatusUpdater(new TestRunAbortedStatusUpdater(
                new InMemoryTestRunStateStorage(TestRun.State.CREATED)))
            .withUserCredentials(userCredentials)
            .withCommandExecutor(new Java8LocalCommandExecutor())
            .destroyEnsembleAfterTest(true);
    }

    protected Path toFullArtifactPath(String relativeOrAbsoluteArtifactPath)
    {
        return testRunArtifactPath().resolve(relativeOrAbsoluteArtifactPath);
    }

    protected void assertArtifactLineMatches(String artifactPath, String lineRegexp)
    {
        assertArtifactExists(artifactPath);

        Path path = toFullArtifactPath(artifactPath);
        String error = "Artifact " + path + " had no line with pattern: " + lineRegexp;
        Pattern p = Pattern.compile(lineRegexp);
        assertThat(this.<Boolean>withArtifactLines(path, lines -> lines.anyMatch(line -> p.matcher(line).find())))
            .withFailMessage(error)
            .isTrue();
    }

    private <Result> Result withArtifactLines(Path fullArtifactPath, Function<Stream<String>, Result> function)
    {
        return Exceptions.getUncheckedIO(() -> {
            try (Stream<String> lines = Files.lines(fullArtifactPath))
            {
                return function.apply(lines);
            }
        });
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
        assertThat(path.toFile()).doesNotExist();
    }

    private void assertArtifactExists(String artifactPath, boolean mustBeEmpty)
    {
        Path path = toFullArtifactPath(artifactPath);
        assertThat(path.toFile())
            .exists();

        Set<String> stringContentFileExts = Set.of("txt", "log", "hdr", "csv", "html", "json", "yaml", "svg");
        String fileExtension = FilenameUtils.getExtension(artifactPath).toLowerCase();
        boolean hasStringContent = stringContentFileExts.contains(fileExtension);
        if (hasStringContent)
        {
            List<String> lines = withArtifactLines(path, lines_ -> lines_.limit(5).collect(Collectors.toList()));
            if (mustBeEmpty)
            {
                String error = "Artifact was not empty: " + path + " first 5 lines:\n" + String.join("\n", lines);
                assertThat(lines)
                    .withFailMessage(error)
                    .isEmpty();
            }
            else
            {
                assertThat(lines)
                    .withFailMessage("Artifact was empty: " + path)
                    .isNotEmpty();
            }
        }
        else
        {
            long size = Exceptions.getUncheckedIO(() -> Files.size(path));
            if (mustBeEmpty)
            {
                assertThat(size)
                    .withFailMessage("Artifact was not empty: " + path)
                    .isEqualTo(0);
            }
            else
            {
                assertThat(size)
                    .withFailMessage("Artifact was empty: " + path)
                    .isNotEqualTo(0);
            }
        }
    }

    protected JsonNode artifactToJson(String artifactPath)
    {
        Path path = toFullArtifactPath(artifactPath);
        return Exceptions.getUncheckedIO(() -> {
            String jsonString = Files.readString(path);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(jsonString);
            assertThat(rootNode.isMissingNode()).isFalse();
            return rootNode;
        });
    }

    protected ActiveTestRun createActiveTestRun(String yaml)
    {
        return createActiveTestRun(yaml, Map.of());
    }

    protected ActiveTestRun createActiveTestRun(String yaml, Map<String, Object> templateParams)
    {
        return createActiveTestRun(yaml, templateParams, DeprecatedPropertiesHandling.LOG_WARNING);
    }

    protected ActiveTestRun createActiveTestRun(String yaml,
        DeprecatedPropertiesHandling deprecatedPropertiesHandling)
    {
        return createActiveTestRun(yaml, Map.of(), deprecatedPropertiesHandling);
    }

    protected ActiveTestRun createActiveTestRun(String yaml, Map<String, Object> templateParams,
        DeprecatedPropertiesHandling deprecatedPropertiesHandling)
    {
        String expandedYaml = TestDefinition.expandTemplate(yaml, templateParams);
        ActiveTestRun activeTestRun = createActiveTestRunBuilder()
            .withTestDefinitionFromYaml(expandedYaml)
            .handleDeprecatedPropertiesWith(deprecatedPropertiesHandling)
            .build();

        assertThat(activeTestRun.getEnsemble()).isNotNull();
        for (NodeGroup nodeGroup : activeTestRun.getEnsemble().getUniqueNodeGroupInstances())
        {
            assertThat(nodeGroup.getState()).isSameAs(NodeGroup.State.UNKNOWN);
        }
        return activeTestRun;
    }

    protected TestResult runYamlFile(String yamlPath)
    {
        return runYaml(readYamlFile(yamlPath), Map.of(), false);
    }

    private TestResult runYaml(String yamlContent, Map<String, Object> templateParams,
        boolean testDependsOnExternalResources)
    {
        ActiveTestRun activeTestRun = createActiveTestRun(yamlContent, templateParams);
        TestResult result = performTestRun(activeTestRun, testDependsOnExternalResources);
        return result;
    }

    protected TestResult performTestRunWithMockedComponents(String testYaml,
        Consumer<MockingComponentFactory> mockingComponentFactoryConsumer)
    {
        final MockingComponentFactory mockingComponentFactory =
            new MockingComponentFactory();
        mockingComponentFactoryConsumer.accept(mockingComponentFactory);

        final String testDefinition = getTestClassResource(testYaml);
        final ActiveTestRun activeTestRun = createActiveTestRunBuilder()
            .withTestDefinitionFromYaml(testDefinition)
            .withComponentFactory(mockingComponentFactory)
            .build();
        return performTestRun(activeTestRun);
    }

    public TestResult performTestRun(ActiveTestRun activeTestRun)
    {
        return performTestRun(activeTestRun, false);
    }

    protected TestResult performTestRun(ActiveTestRun activeTestRun, boolean testDependsOnExternalResources)
    {
        activeTestRun.run(logger::error);

        final String resourceFailureMessage = testDependsOnExternalResources ?
            "Insufficient resources to run test; please re-run" :
            "Test should not have been requeued";

        assertThat(activeTestRun.getTestRunState())
            .withFailMessage(resourceFailureMessage)
            .isNotSameAs(TestRun.State.WAITING_FOR_RESOURCES);

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

    protected boolean maybeRunExpensiveTest(String yamlPath)
    {
        return maybeRunTest(TestHelpers::runExpensiveTests, yamlPath);
    }

    protected boolean maybeRunTestThatCostsMoney(String yamlPath)
    {
        return maybeRunTest(TestHelpers::runTestsThatCostMoney, yamlPath);
    }

    /** Run the test if the specified predicate returns true, returning whether the test was run successfully */
    protected boolean maybeRunTest(BooleanSupplier predicate, String yamlPath)
    {
        if (predicate.getAsBoolean())
        {
            return maybeAssertYamlFileRunsAndPasses(yamlPath);
        }
        else
        {
            assertYamlFileValidates(yamlPath);
        }
        return false;
    }

    protected boolean maybeAssertYamlFileRunsAndPasses(String yamlPath)
    {
        return maybeAssertYamlFileRunsAndPasses(yamlPath, Map.of());
    }

    protected boolean maybeAssertYamlFileRunsAndPasses(String yamlPath, Map<String, Object> templateParams)
    {
        return maybeAssertYamlFileRunsWithResult(yamlPath, templateParams, true);
    }

    protected boolean maybeAssertYamlFileRunsAndFails(String yamlPath)
    {
        return maybeAssertYamlFileRunsWithResult(yamlPath, Map.of(), false);
    }

    /** Validate the file; run and check the result if:
     *
     *   - the file is a ctool test and runExpensiveTests is set OR
     *   - the file is a ccm test and skipCCMTests is not set
     *
     *   returns true if the test was run, false if it was only validated
     */
    private boolean maybeAssertYamlFileRunsWithResult(
        String yamlPath, Map<String, Object> templateParams, boolean expectedRunResult)
    {
        String yamlContent = readYamlFile(yamlPath);

        boolean isCToolTest =
            yamlPath.toLowerCase().contains("ctool") ||
                yamlContent.contains("name: ctool");

        boolean isCCMTest =
            yamlPath.toLowerCase().contains("ccm") ||
                yamlContent.contains("name: ccm");

        boolean canRun =
            (!isCToolTest || TestHelpers.runExpensiveTests()) &&
                (!isCCMTest || TestHelpers.runCCMTests());

        if (canRun)
        {
            assertYamlRunsWithResult(yamlContent, templateParams, expectedRunResult, isCToolTest);
            return true;
        }
        else
        {
            // even if we dont run, the yaml must still parse successfully
            assertYamlValidates(yamlContent);
            return false;
        }
    }

    protected void assertYamlRunsWithResult(
        String yamlContent, Map<String, Object> templateParams, boolean expectedRunResult,
        boolean testDependsOnExternalResources)
    {
        boolean runResult = runYaml(yamlContent, templateParams, testDependsOnExternalResources).isValid();
        assertThat(runResult).isEqualTo(expectedRunResult);
    }

    protected void assertYamlFileValidates(String yamlPath)
    {
        assertYamlValidates(readYamlFile(yamlPath));
    }

    private void assertYamlValidates(String yamlContent)
    {
        assertThat(createActiveTestRun(yamlContent)).isNotNull();
    }
}
