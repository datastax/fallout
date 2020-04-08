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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;

import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.TestDefinition;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.FalloutServiceRule;
import com.datastax.fallout.service.resources.FalloutServiceRule.FalloutServiceResetRule;
import com.datastax.fallout.service.resources.RestApiBuilder;

import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.core.TestAssert.assertThat;
import static com.datastax.fallout.service.core.TestRun.State.ABORTED;
import static com.datastax.fallout.service.core.TestRun.State.FAILED;
import static com.datastax.fallout.service.core.TestRun.State.PASSED;
import static com.datastax.fallout.service.core.TestRun.State.RUNNING;
import static com.datastax.fallout.service.core.TestRunAssert.assertThat;
import static javax.ws.rs.core.Response.Status.*;
import static javax.ws.rs.core.ResponseAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

public class TestResourceHttpTest
{
    public static abstract class Tests extends TestResourceTestBase
    {
        private RestApiBuilder adminApi;

        protected abstract FalloutServiceRule getFalloutServiceRule();

        protected abstract FalloutServiceResetRule getFalloutServiceResetRule();

        @Before
        public void setupRestApiBuilders()
        {
            api = getFalloutServiceResetRule().userApi();
            adminApi = getFalloutServiceResetRule().adminApi();
        }

        @org.junit.Test
        public void fake_test_can_be_created_and_run() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);
            assertThat(runTest("fakes")).hasState(PASSED);
        }

        @org.junit.Test
        public void newly_created_tests_do_not_cause_NPEs_on_user_page()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            assertThat(api.build(TestResource.class, "showTests", TEST_USER_EMAIL).get())
                .hasStatusInfo(OK);
        }

        @org.junit.Test
        public void newly_created_tests_have_size_on_disk_bytes_set_to_zero()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            assertThat(api.build(TestResource.class, "getTestForUserApi", "fakes").get(Test.class))
                .hasName("fakes")
                .hasSizeOnDiskBytes(0L);
        }

        @org.junit.Test
        public void fake_test_can_be_edited_and_run() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);
            assertThat(editTest("/", "fakes-edited", "fakes")).hasStatusInfo(OK);
            assertThat(runTest("fakes")).hasState(PASSED);
        }

        @org.junit.Test
        public void fake_test_can_be_created_via_edit_and_run() throws IOException
        {
            assertThat(editTest("/", "fakes-edited", "fakes")).hasStatusInfo(CREATED);
            assertThat(runTest("fakes")).hasState(PASSED);
        }

        @org.junit.Test
        public void default_template_values_are_used() throws IOException
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);
            assertThat(runTest("complete-defaults")).hasState(PASSED);
        }

        @org.junit.Test
        public void
            templated_test_run_displays_default_and_actual_template_params_and_displayed_yaml_can_be_run_correctly()
                throws IOException
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);

            TestRun testRun = runTest(
                "complete-defaults",
                "dummy_module: noop\nrunlevel: STARTED_SERVICES_RUNNING\nchecker_module: nofail");
            assertThat(testRun).hasState(PASSED);

            String definitionWithTemplateParams = testRun.getDefinitionWithTemplateParams();

            String expectedTestRunDefinition = String.format(
                EnsembleFalloutTest.readYamlFile("/template-test-yamls/complete-defaults-with-run-params.yaml"),
                testRun.getTestRunId().toString()
            );
            assertThat(expectedTestRunDefinition).isEqualTo(definitionWithTemplateParams);

            assertThat(
                createTest("/template-test-yamls/", "complete-defaults-with-run-params")).hasStatusInfo(CREATED);
            TestRun testRun2 = runTest("complete-defaults-with-run-params");
            assertThat(testRun2).hasState(PASSED);
            assertThat(testRun2.getTemplateParamsMap().get("dummy_module")).isEqualTo("noop");
        }

        private static Pair<Optional<String>, Optional<String>>
            getTestRunDefaultsAndParams(Optional<String> testRunDefaults)
        {
            return testRunDefaults
                .map(testRunDefaults_ -> {
                    String[] defaultsAndParams = testRunDefaults_.split("(\\n|\\A)# PARAMETERS FOR TEST RUN[^\\n]+\\n");
                    if (defaultsAndParams.length == 1)
                    {
                        return Pair.of(Optional.of(defaultsAndParams[0]), Optional.<String>empty());
                    }
                    assertThat(defaultsAndParams.length).isEqualTo(2);
                    return Pair.of(
                        Optional.of(defaultsAndParams[0]).flatMap(s -> s.isEmpty() ? Optional.empty() : Optional.of(s)),
                        Optional.of(defaultsAndParams[1]));
                })
                .orElse(Pair.of(Optional.empty(), Optional.empty()));
        }

        @org.junit.Test
        public void untemplated_test_run_displays_params_if_present() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            final String templateParams = "foo: bar";
            final TestRun testRun = runTest("fakes", templateParams);
            assertThat(testRun).hasState(PASSED);

            final String originalDefinition =
                EnsembleFalloutTest.readYamlFile("/fakes.yaml");

            final Pair<Optional<String>, String> defaultsYamlAndDefinitionYaml = TestDefinition
                .splitDefaultsAndDefinition(testRun.getDefinitionWithTemplateParams());

            final Pair<Optional<String>, Optional<String>> testRunDefaultsAndParams = getTestRunDefaultsAndParams(
                defaultsYamlAndDefinitionYaml.getLeft());

            final Optional<String> testRunDefaults = testRunDefaultsAndParams.getLeft();
            assertThat(testRunDefaults).isEmpty();

            final Optional<String> testRunParams = testRunDefaultsAndParams.getRight();
            assertThat(testRunParams).contains(templateParams + "\n");

            String testRunDefinition = defaultsYamlAndDefinitionYaml.getRight();
            assertThat(testRunDefinition).isEqualTo(originalDefinition);
        }

        @org.junit.Test
        public void test_run_does_not_display_template_params_if_no_params() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            assertThat(EnsembleFalloutTest.readYamlFile("/fakes.yaml"))
                .isEqualTo(testRun.getDefinitionWithTemplateParams());
        }

        @org.junit.Test
        public void create_test_fails_on_missing_default_template_tag_values() throws IOException
        {
            final Response response = createTest("/template-test-yamls/", "partial-defaults");
            assertThat(response).hasStatusInfo(BAD_REQUEST);
            assertThat(response.readEntity(String.class)).contains("no_default_provided_for_this_tag");
        }

        @org.junit.Test
        public void tag_values_can_be_overridden_using_json() throws IOException
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);
            assertThat(runTest("complete-defaults", ImmutableMap.of("dummy_module", "fail")))
                .hasState(FAILED);
        }

        @org.junit.Test
        public void tag_values_can_be_overridden_using_yaml() throws IOException
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);
            assertThat(runTest("complete-defaults", "dummy_module: fail"))
                .hasState(FAILED);
        }

        @org.junit.Test
        public void tag_values_can_be_overridden_using_query_params() throws IOException
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);

            final Response response =
                api.target("/tests/complete-defaults/runs/api").queryParam("dummy_module", "fail").request().post(null);
            assertThat(response).hasStatusInfo(CREATED);
            assertThat(waitForCompletedTestRun(response.readEntity(TestRun.class))).hasState(FAILED);
        }

        public void assertThatRerunTestrunWithCloneUsesTheSpecifiedDefinition(boolean clone, String definitionPath)
            throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            final TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);
            assertThat(testRun.getDefinition()).isEqualTo(EnsembleFalloutTest.readYamlFile("/fakes.yaml"));

            assertThat(editTest("/", "fakes-edited", "fakes")).hasStatusInfo(OK);

            final TestRun cloned = reRunTestWithClone(testRun, clone);
            assertThat(cloned).hasState(PASSED);
            assertThat(cloned.getDefinition()).isEqualTo(EnsembleFalloutTest.readYamlFile(definitionPath));
        }

        @org.junit.Test
        public void rerun_testrun_with_clone_uses_the_definition_of_the_testrun_and_not_the_test() throws IOException
        {
            assertThatRerunTestrunWithCloneUsesTheSpecifiedDefinition(true, "/fakes.yaml");
        }

        @org.junit.Test
        public void rerun_testrun_without_clone_uses_the_definition_of_the_test() throws IOException
        {
            assertThatRerunTestrunWithCloneUsesTheSpecifiedDefinition(false, "/fakes-edited.yaml");
        }

        public void assertThatRerunTestrunReusesTheTemplateParameters(boolean clone) throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            final String templateParams = "foo: bar";

            final TestRun testRun = runTest("fakes", templateParams);
            assertThat(testRun).hasState(PASSED);
            assertThat(testRun).hasTemplateParamsMap(ImmutableMap.of("foo", "bar"));

            final TestRun cloned = reRunTestWithClone(testRun, clone);
            assertThat(cloned).hasState(PASSED);
            assertThat(cloned).hasTemplateParamsMap(ImmutableMap.of("foo", "bar"));
        }

        @org.junit.Test
        public void rerun_testrun_with_clone_reuses_the_template_parameters() throws IOException
        {
            assertThatRerunTestrunReusesTheTemplateParameters(true);
        }

        @org.junit.Test
        public void rerun_testrun_without_clone_reuses_the_template_parameters() throws IOException
        {
            assertThatRerunTestrunReusesTheTemplateParameters(false);
        }

        @org.junit.Test
        public void abort_and_requeue_running_test_runs_works() throws IOException
        {
            // create test
            Response response = createTest("/testrunner-test-yamls/", "abortable-sleep");
            assertThat(response).hasStatusInfo(CREATED);
            Test test = response.readEntity(Test.class);

            final int numberOfTestRuns = 3;

            // create testruns with different parameters
            List<TestRun> originalTestRuns = IntStream.range(0, numberOfTestRuns)
                .mapToObj(param -> startTest(test.getName(), "param: " + param))
                .collect(Collectors.toList());

            // wait for them to start
            originalTestRuns = waitForTestRunsToSatisfy(originalTestRuns, testRun -> testRun.getState() == RUNNING);

            // stop the testrunner
            assertThat(
                adminApi
                    .build(TestResource.class, "requestShutdownAPI")
                    .post(null))
                        .hasStatusInfo(NO_CONTENT);

            // check that the original testruns are listed
            List<TestRun> allTestRuns = api
                .build(TestResource.class, "listTestRunsApi", test.getOwner(), test.getName())
                .get(new GenericType<List<TestRun>>()
                {
                });
            assertThat(allTestRuns).containsExactlyInAnyOrderElementsOf(originalTestRuns);

            // abort and requeue
            response = adminApi.build(TestResource.class, "abortAndRequeueRunningTestRuns").post(null);
            assertThat(response).hasStatusInfo(OK);
            List<TestRun> requeued = response.readEntity(new GenericType<List<TestRun>>()
            {
            });

            // check that new testruns have the same template params
            assertThat(
                requeued.stream()
                    .map(testRun -> (Integer) testRun.getTemplateParamsMap().get("param"))
                    .collect(Collectors.toList()))
                        .containsExactlyInAnyOrder(
                            IntStream.range(0, numberOfTestRuns).boxed().toArray(Integer[]::new));

            // ...but not the same IDs
            assertThat(
                requeued.stream()
                    .map(testRun -> testRun.getTestRunId())
                    .collect(Collectors.toList()))
                        .doesNotContainAnyElementsOf(originalTestRuns.stream()
                            .map(TestRun::getTestRunId).collect(Collectors.toList()));

            // check that the list of testruns contains the new testruns
            allTestRuns = api
                .build(TestResource.class, "listTestRunsApi", test.getOwner(), test.getName())
                .get(new GenericType<List<TestRun>>()
                {
                });
            assertThat(allTestRuns).containsAll(requeued);

            // check that existing testruns are aborted
            originalTestRuns = waitForTestRunsToSatisfy(originalTestRuns, testRun -> testRun.getState().finished());

            assertThat(originalTestRuns).allSatisfy(testRun -> assertThat(testRun).hasState(ABORTED));

            // Restart the queue
            assertThat(adminApi.build(TestResource.class, "cancelShutdown").post(null)).hasStatusInfo(NO_CONTENT);

            // Wait for requeued test runs to complete
            assertThat(waitForTestRunsToSatisfy(requeued, testRun -> testRun.getState().finished()))
                .allSatisfy(testRun -> assertThat(testRun).hasState(PASSED));
        }

        @org.junit.Test
        public void delete_test_runs_works() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            Response response = api.build(TestResource.class, "deleteTestRun", testRun.getOwner(), "fakes",
                testRun.getTestRunId().toString()).delete();
            assertThat(response).hasStatusInfo(OK);
            assertThat(api.build(TestResource.class, "getDeletedTestRunApi", testRun.getOwner(), "fakes",
                testRun.getTestRunId().toString()).get()).hasStatusInfo(OK);
            assertThat(api.build(TestResource.class, "getTestRunApi", testRun.getOwner(), "fakes",
                testRun.getTestRunId().toString()).get()).hasStatusInfo(NOT_FOUND);
        }

        @org.junit.Test
        public void delete_test_works() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            assertThat(api.build(TestResource.class, "deleteTestForUserApi", "fakes").delete()).hasStatusInfo(OK);
            assertThat(api.build("tests/deleted/" + TEST_USER_EMAIL + "/fakes/api").get()).hasStatusInfo(OK);
            assertThat(api.build(TestResource.class, "getDeletedTestRunApi", testRun.getOwner(), "fakes",
                testRun.getTestRunId().toString()).get()).hasStatusInfo(OK);
            assertThat(api.build("tests/" + TEST_USER_EMAIL + "/fakes/api").get()).hasStatusInfo(NOT_FOUND);
        }

        @org.junit.Test
        public void restore_test_works() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            for (int x = 0; x < 3; x++)
            {
                assertThat(api.build(TestResource.class, "deleteTestForUserApi", "fakes").delete()).hasStatusInfo(OK);
                assertThat(api.build(TestResource.class, "restoreTest", TEST_USER_EMAIL, "fakes").post(null))
                    .hasStatusInfo(OK);

                assertThat(api.build(TestResource.class, "getDeletedTestForUserApi", "fakes").get())
                    .hasStatusInfo(NOT_FOUND);
                assertThat(api.build(TestResource.class, "getDeletedTestRunApi", testRun.getOwner(), "fakes",
                    testRun.getTestRunId().toString()).get()).hasStatusInfo(NOT_FOUND);
                assertThat(api.build(TestResource.class, "getTestForUserApi", "fakes").get()).hasStatusInfo(OK);
                assertThat(api.build(TestResource.class, "getTestRunApi", testRun.getOwner(), "fakes",
                    testRun.getTestRunId().toString()).get()).hasStatusInfo(OK);
            }
        }

        @org.junit.Test
        public void restore_test_run_works() throws IOException
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            for (int x = 0; x < 3; x++)
            {
                assertThat(api.build(TestResource.class, "deleteTestForUserApi", "fakes").delete()).hasStatusInfo(OK);
                assertThat(api.build(TestResource.class, "getDeletedTestRunApi", testRun.getOwner(), "fakes",
                    testRun.getTestRunId().toString()).get()).hasStatusInfo(OK);
                assertThat(api.build(TestResource.class, "restoreTestRun", testRun.getOwner(), "fakes",
                    testRun.getTestRunId().toString()).post(null)).hasStatusInfo(OK);

                assertThat(api.build(TestResource.class, "getDeletedTestRunApi", testRun.getOwner(), "fakes",
                    testRun.getTestRunId().toString()).get()).hasStatusInfo(NOT_FOUND);
                assertThat(api.build(TestResource.class, "getTestRunApi", testRun.getOwner(), "fakes",
                    testRun.getTestRunId().toString()).get()).hasStatusInfo(OK);
                assertThat(api.build(TestResource.class, "getDeletedTestForUserApi", "fakes").get())
                    .hasStatusInfo(NOT_FOUND);
                assertThat(api.build(TestResource.class, "getTestForUserApi", "fakes").get()).hasStatusInfo(OK);
            }
        }

        @org.junit.Test
        public void delete_deleted_test_forever_works() throws IOException
        {
            Response test = createTest("/", "fakes");
            assertThat(test).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            assertThat(api.build(TestResource.class, "deleteTestForUserApi", "fakes").delete()).hasStatusInfo(OK);
            assertThat(api.build(TestResource.class, "deleteDeletedTestForever", TEST_USER_EMAIL, "fakes").delete())
                .hasStatusInfo(OK);
            assertThat(api.build(TestResource.class, "getDeletedTestForUserApi", "fakes").get())
                .hasStatusInfo(NOT_FOUND);
            assertThat(api.build(TestResource.class, "getDeletedTestRunApi", testRun.getOwner(), "fakes",
                testRun.getTestRunId().toString()).get()).hasStatusInfo(NOT_FOUND);
        }

        @org.junit.Test
        public void download_artifacts_as_zip_works() throws IOException
        {
            //create and run test
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);
            //rename artifact
            String artifactLocation = Paths
                .get(Artifacts.buildTestRunArtifactPath(getFalloutServiceRule().getArtifactPath(), testRun).toString())
                .toString();
            Path sharedLog = Paths.get(artifactLocation, "fallout-shared.log");
            assertThat(sharedLog).exists();

            Path changedName = Paths.get(artifactLocation, "renamed.log");
            Files.move(sharedLog, changedName);

            //download artifacts
            Response response = api.build(TestResource.class, "downloadArtifactsAsZip", testRun.getOwner(),
                testRun.getTestName(), testRun.getTestRunId().toString()).get();
            assertThat(response).hasStatusInfo(OK);

            ZipInputStream zipInputStream = new ZipInputStream(response.readEntity(InputStream.class));
            ArrayList<String> downloadedArtifacts = new ArrayList<>();

            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null)
            {
                downloadedArtifacts.add(entry.getName());
            }
            assertThat(downloadedArtifacts).isNotEmpty();

            String relativePath = String
                .format("%s/%s/%s/", testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId());
            assertThat(downloadedArtifacts).contains(relativePath + changedName.getFileName());
            assertThat(downloadedArtifacts).doesNotContain(relativePath + sharedLog.getFileName());
        }
    }

    public static class UsingStandaloneServer extends Tests
    {
        @ClassRule
        public static final FalloutServiceRule FALLOUT_SERVICE_RULE =
            new FalloutServiceRule(FalloutConfiguration.ServerMode.STANDALONE);

        @Rule
        public final FalloutServiceResetRule FALLOUT_SERVICE_RESET_RULE =
            FALLOUT_SERVICE_RULE.resetRule();

        @Override
        protected FalloutServiceRule getFalloutServiceRule()
        {
            return FALLOUT_SERVICE_RULE;
        }

        @Override
        protected FalloutServiceResetRule getFalloutServiceResetRule()
        {
            return FALLOUT_SERVICE_RESET_RULE;
        }
    }

    public static class UsingQueueServer extends Tests
    {
        @ClassRule
        public static final FalloutServiceRule FALLOUT_SERVICE_RULE =
            new FalloutServiceRule(FalloutConfiguration.ServerMode.QUEUE);

        @Rule
        public final FalloutServiceResetRule FALLOUT_SERVICE_RESET_RULE =
            FALLOUT_SERVICE_RULE.resetRule();

        @Override
        protected FalloutServiceRule getFalloutServiceRule()
        {
            return FALLOUT_SERVICE_RULE;
        }

        @Override
        protected FalloutServiceResetRule getFalloutServiceResetRule()
        {
            return FALLOUT_SERVICE_RESET_RULE;
        }
    }
}
