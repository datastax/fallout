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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.TestDefinition;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.FalloutAppExtension;
import com.datastax.fallout.service.resources.RestApiBuilder;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.core.Fakes.TEST_NAME;
import static com.datastax.fallout.service.core.Fakes.TEST_RUN_ID;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.core.TestRun.State.FAILED;
import static com.datastax.fallout.service.core.TestRun.State.PASSED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CREATED;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

public class TestResourceHttpTest
{
    public static abstract class Tests extends TestResourceTestBase<FalloutAppExtension>
    {
        private RestApiBuilder adminApi;

        protected Tests(FalloutAppExtension falloutAppExtension)
        {
            super(falloutAppExtension);
        }

        @BeforeEach
        public void setupRestApiBuilders()
        {
            api = getFalloutServiceResetExtension().userApi();
            adminApi = getFalloutServiceResetExtension().adminApi();
        }

        @org.junit.jupiter.api.Test
        public void fake_test_can_be_created_and_run()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);
            assertThat(runTest("fakes")).hasState(PASSED);
        }

        @org.junit.jupiter.api.Test
        public void newly_created_tests_do_not_cause_NPEs_on_user_page()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            assertThat(api.build(TestResource.class, "showTests", TEST_USER_EMAIL).get())
                .hasStatusInfo(OK);
        }

        @org.junit.jupiter.api.Test
        public void newly_created_tests_have_size_on_disk_bytes_set_to_zero()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            assertThat(api.build(TestResource.class, "getTestForUserApi", "fakes").get(Test.class))
                .hasName("fakes")
                .hasSizeOnDiskBytes(0L);
        }

        @org.junit.jupiter.api.Test
        public void fake_test_can_be_edited_and_run()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);
            assertThat(editTest("/", "fakes-edited", "fakes")).hasStatusInfo(OK);
            assertThat(runTest("fakes")).hasState(PASSED);
        }

        @org.junit.jupiter.api.Test
        public void fake_test_can_be_created_via_edit_and_run()
        {
            assertThat(editTest("/", "fakes-edited", "fakes")).hasStatusInfo(CREATED);
            assertThat(runTest("fakes")).hasState(PASSED);
        }

        @org.junit.jupiter.api.Test
        public void default_template_values_are_used()
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);
            assertThat(runTest("complete-defaults")).hasState(PASSED);
        }

        @org.junit.jupiter.api.Test
        public void
            templated_test_run_displays_default_and_actual_template_params_and_displayed_yaml_can_be_run_correctly()
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);

            TestRun testRun = runTest(
                "complete-defaults",
                "dummy_module: noop\nrunlevel: STARTED_SERVICES_RUNNING\nchecker_module: nofail");
            assertThat(testRun).hasState(PASSED);

            String definitionWithTemplateParams = testRun.getDefinitionWithTemplateParams();

            String expectedTestRunDefinition = String.format(
                EnsembleFalloutTest.readSharedYamlFile("/template-test-yamls/complete-defaults-with-run-params.yaml"),
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

        @org.junit.jupiter.api.Test
        public void untemplated_test_run_displays_params_if_present()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            final String templateParams = "foo: bar";
            final TestRun testRun = runTest("fakes", templateParams);
            assertThat(testRun).hasState(PASSED);

            final String originalDefinition =
                EnsembleFalloutTest.readSharedYamlFile("/fakes.yaml");

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

        @org.junit.jupiter.api.Test
        public void test_run_does_not_display_template_params_if_no_params()
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            assertThat(EnsembleFalloutTest.readSharedYamlFile("/fakes.yaml"))
                .isEqualTo(testRun.getDefinitionWithTemplateParams());
        }

        @org.junit.jupiter.api.Test
        public void create_test_fails_on_missing_default_template_tag_values()
        {
            final Response response = createTest("/template-test-yamls/", "partial-defaults");
            assertThat(response).hasStatusInfo(BAD_REQUEST);
            assertThat(response.readEntity(String.class)).contains("no_default_provided_for_this_tag");
        }

        @org.junit.jupiter.api.Test
        public void tag_values_can_be_overridden_using_json()
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);
            assertThat(runTest("complete-defaults", Map.of("dummy_module", "fail")))
                .hasState(FAILED);
        }

        @org.junit.jupiter.api.Test
        public void tag_values_can_be_overridden_using_yaml()
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);
            assertThat(runTest("complete-defaults", "dummy_module: fail"))
                .hasState(FAILED);
        }

        @org.junit.jupiter.api.Test
        public void tag_values_can_be_overridden_using_query_params()
        {
            assertThat(createTest("/template-test-yamls/", "complete-defaults")).hasStatusInfo(CREATED);

            final Response response =
                api.target("/tests/complete-defaults/runs/api").queryParam("dummy_module", "fail").request().post(null);
            assertThat(response).hasStatusInfo(CREATED);
            assertThat(waitForCompletedTestRun(response.readEntity(TestRun.class))).hasState(FAILED);
        }

        public void assertThatRerunTestrunWithCloneUsesTheSpecifiedDefinition(boolean clone, String definitionPath)
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            final TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);
            assertThat(testRun.getDefinition()).isEqualTo(EnsembleFalloutTest.readSharedYamlFile("/fakes.yaml"));

            assertThat(editTest("/", "fakes-edited", "fakes")).hasStatusInfo(OK);

            final TestRun cloned = reRunTestWithClone(testRun, clone);
            assertThat(cloned).hasState(PASSED);
            assertThat(cloned.getDefinition()).isEqualTo(EnsembleFalloutTest.readSharedYamlFile(definitionPath));
        }

        @org.junit.jupiter.api.Test
        public void rerun_testrun_with_clone_uses_the_definition_of_the_testrun_and_not_the_test()
        {
            assertThatRerunTestrunWithCloneUsesTheSpecifiedDefinition(true, "/fakes.yaml");
        }

        @org.junit.jupiter.api.Test
        public void rerun_testrun_without_clone_uses_the_definition_of_the_test()
        {
            assertThatRerunTestrunWithCloneUsesTheSpecifiedDefinition(false, "/fakes-edited.yaml");
        }

        public void assertThatRerunTestrunReusesTheTemplateParameters(boolean clone)
        {
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            final String templateParams = "foo: bar";

            final TestRun testRun = runTest("fakes", templateParams);
            assertThat(testRun).hasState(PASSED);
            assertThat(testRun).hasTemplateParamsMap(Map.of("foo", "bar"));

            final TestRun cloned = reRunTestWithClone(testRun, clone);
            assertThat(cloned).hasState(PASSED);
            assertThat(cloned).hasTemplateParamsMap(Map.of("foo", "bar"));
        }

        @org.junit.jupiter.api.Test
        public void rerun_testrun_with_clone_reuses_the_template_parameters()
        {
            assertThatRerunTestrunReusesTheTemplateParameters(true);
        }

        @org.junit.jupiter.api.Test
        public void rerun_testrun_without_clone_reuses_the_template_parameters()
        {
            assertThatRerunTestrunReusesTheTemplateParameters(false);
        }

        @org.junit.jupiter.api.Test
        public void delete_test_runs_works()
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

        @org.junit.jupiter.api.Test
        public void delete_test_works()
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

        @org.junit.jupiter.api.Test
        public void restore_test_works()
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

        @org.junit.jupiter.api.Test
        public void restore_test_run_works()
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

        @org.junit.jupiter.api.Test
        public void restoring_a_non_existent_deleted_test_run_returns_404()
        {
            assertThat(api.build(TestResource.class, "restoreTestRun",
                TEST_USER_EMAIL, TEST_NAME, TEST_RUN_ID.toString()).post(null))
                    .hasStatusInfo(NOT_FOUND);
        }

        @org.junit.jupiter.api.Test
        public void delete_deleted_test_forever_works()
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

        @org.junit.jupiter.api.Test
        public void download_artifacts_as_zip_works() throws IOException
        {
            //create and run test
            assertThat(createTest("/", "fakes")).hasStatusInfo(CREATED);

            TestRun testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);
            //rename artifact
            String artifactLocation = Paths
                .get(Artifacts.buildTestRunArtifactPath(getFalloutApp().getArtifactPath(), testRun)
                    .toString())
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
        @RegisterExtension
        public static final FalloutAppExtension FALLOUT_SERVICE =
            new FalloutAppExtension(FalloutConfiguration.ServerMode.STANDALONE);

        public UsingStandaloneServer()
        {
            super(FALLOUT_SERVICE);
        }

        @org.junit.jupiter.api.Test
        public void updating_artifacts_updates_the_artifact_list_and_the_test_size()
        {
            final var test = createTest("/", "fakes").readEntity(Test.class);

            var testRun = runTest("fakes");
            assertThat(testRun).hasState(PASSED);

            final var newArtifact = "surprise.txt";

            final var newArtifactPath =
                Artifacts.buildTestRunArtifactPath(getFalloutApp().getArtifactPath(), testRun)
                    .resolve(newArtifact);

            Supplier<TestRun> getTestRunWithUpdatedArtifacts = () -> {
                assertThat(api
                    .build(TestResource.class, "updateFinishedTestRunArtifacts",
                        testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId())
                    .post(null)
                    .getStatusInfo())
                        .hasFamily(Response.Status.Family.SUCCESSFUL);

                return api.build(TestResource.uriForGetTestRunApi(testRun).getPath()).get(TestRun.class);
            };

            Supplier<Test> getTest =
                () -> api.build(TestResource.class, "getTestApi", test.getOwner(), test.getName())
                    .get(Test.class);

            final var originalTestRunSize = testRun.getArtifactsSizeBytes().get();

            assertThat(testRun.getArtifacts()).doesNotContainKey(newArtifact);

            final var newArtifactContent = "boo!";
            FileUtils.writeString(newArtifactPath, newArtifactContent);

            assertThat(getTestRunWithUpdatedArtifacts.get()).satisfies(testRun_ -> {
                assertThat(testRun_.getArtifacts()).containsKey(newArtifact);
                assertThat(testRun_.getArtifactsSizeBytes())
                    .hasValue(originalTestRunSize + newArtifactContent.length())
                    .hasValue(getTest.get().getSizeOnDiskBytes());
            });

            Exceptions.runUncheckedIO(() -> Files.delete(newArtifactPath));

            assertThat(getTestRunWithUpdatedArtifacts.get()).satisfies(testRun_ -> {
                assertThat(testRun_.getArtifacts()).doesNotContainKey(newArtifact);
                assertThat(testRun_.getArtifactsSizeBytes())
                    .hasValue(originalTestRunSize)
                    .hasValue(getTest.get().getSizeOnDiskBytes());
            });
        }
    }

    public static class UsingQueueServer extends Tests
    {
        @RegisterExtension
        public static final FalloutAppExtension FALLOUT_SERVICE =
            new FalloutAppExtension(FalloutConfiguration.ServerMode.QUEUE);

        public UsingQueueServer()
        {
            super(FALLOUT_SERVICE);
        }

        @org.junit.jupiter.api.Test
        public void running_a_test_with_a_corrupted_user_fails()
        {
            final var corruptUserApi = getFalloutServiceResetExtension().corruptUserApi();

            assertThat(createTest(corruptUserApi, "/", "fakes"))
                .hasStatusInfo(CREATED);

            final var testRun = runTest(corruptUserApi, "fakes");
            assertThat(testRun).hasState(FAILED);
        }
    }
}
