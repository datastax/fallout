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
package com.datastax.fallout.components.kubernetes;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.common.configuration_manager.NoopConfigurationManager;
import com.datastax.fallout.harness.MockCommandExecutor;
import com.datastax.fallout.ops.EnsembleCredentials;
import com.datastax.fallout.ops.JobConsoleLoggers;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.util.MustacheFactoryWithoutHTMLEscaping;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.harness.MockCommandExecutor.command;
import static com.datastax.fallout.runner.CheckResourcesResult.AVAILABLE;
import static com.datastax.fallout.runner.CheckResourcesResult.FAILED;

class GoogleKubernetesEngineProvisionerTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    private MockCommandExecutor mockCommandExecutor;

    final Collection<String> loggedErrors = new ConcurrentLinkedQueue<>();

    private MockCommandExecutor.MockCommandResponse kubectlGetNodesCommand;
    private static String kubectlGetNodesOutput;

    private MockCommandExecutor.MockCommandResponse kubectlGetPersistentVolumesCommand;

    private MockCommandExecutor.MockCommandResponse gcloudComputeDisksListCommand;
    private static String gcloudComputeDisksListOutput;

    private class ErrorCollectingJobConsoleLoggers extends JobConsoleLoggers
    {
        @Override
        public Logger create(String name, Path ignored)
        {
            final var logger = (ch.qos.logback.classic.Logger) super.create(name, ignored);

            final var loggerContext = logger.getLoggerContext();

            final var appender = new AppenderBase<ILoggingEvent>() {
                @Override
                protected void append(ILoggingEvent event)
                {
                    if (event.getLevel() == Level.ERROR)
                    {
                        loggedErrors.add(event.getFormattedMessage());
                    }
                }
            };
            appender.setContext(loggerContext);
            appender.start();

            logger.addAppender(appender);
            return logger;
        }
    }

    private static String kubectlGetPersistentVolumesOutputSecondDiskIsDynamicallyAllocated(
        boolean secondDiskIsDynamicallyAllocated)
    {
        return MustacheFactoryWithoutHTMLEscaping.renderWithScopes(
            getTestClassResourceAsString("kubectl-get-persistentvolumes.json" + ".mustache"), List.of(
                new HashMap<String, Object>(Map.of(
                    "secondDiskIsDynamicallyAllocated", secondDiskIsDynamicallyAllocated
                ))));
    }

    @BeforeAll
    static void beforeAll()
    {
        // All the files used in this test are taken from a real run of the relevant commands,
        // and then tweaked with mustache if necessary to get the outputs needed to exercise the logic.
        kubectlGetNodesOutput = getTestClassResourceAsString("kubectl-get-nodes.json");

        gcloudComputeDisksListOutput = getTestClassResourceAsString("gcloud-compute-disks-list.json");
    }

    @BeforeEach
    void setUp()
    {
        kubectlGetNodesCommand = command("kubectl get nodes -o json")
            .outputsOnStdout(kubectlGetNodesOutput);

        kubectlGetPersistentVolumesCommand = command("kubectl get persistentvolumes -o json")
            .outputsOnStdout(kubectlGetPersistentVolumesOutputSecondDiskIsDynamicallyAllocated(true));

        gcloudComputeDisksListCommand = command("gcloud compute disks list --project fake --format json")
            .outputsOnStdout(gcloudComputeDisksListOutput);
    }

    void createAndDestroyNodegroup(CheckResourcesResult expectedResult)
    {
        final var provisioner = new GoogleKubernetesEngineProvisioner();

        mockCommandExecutor = new MockCommandExecutor(
            kubectlGetNodesCommand,
            kubectlGetPersistentVolumesCommand,
            gcloudComputeDisksListCommand
        );

        provisioner.setLocalCommandExecutor(mockCommandExecutor);

        final var properties = new WritablePropertyGroup();
        properties.with(provisioner.prefix())
            .put("name", "fake")
            .put("project", "fake")
            .put("zone", "fake")
            .put("machine.type", "fake");

        final var nodeGroup = NodeGroupBuilder.create()
            .withName("gke-test")
            .withNodeCount(3)
            .withPropertyGroup(properties)
            .withProvisioner(provisioner)
            .withConfigurationManager(new NoopConfigurationManager())
            .withTestRunArtifactPath(persistentTestClassOutputDir())
            .withTestRunScratchSpace(persistentTestClassScratchSpace())
            .withCredentials(new EnsembleCredentials(
                new UserCredentials(getTestUser(), Optional.empty()),
                falloutConfiguration()))
            .withLoggers(new ErrorCollectingJobConsoleLoggers())
            .build();

        assertThat(nodeGroup.transitionState(NodeGroup.State.STARTED_SERVICES_RUNNING).join()).wasSuccessful();
        assertThat(nodeGroup.transitionState(NodeGroup.State.DESTROYED).join()).isEqualTo(expectedResult);

        if (expectedResult.wasSuccessful())
        {
            assertThat(loggedErrors).isEmpty();
        }
        else
        {
            assertThat(loggedErrors).isNotEmpty();
        }
    }

    @Test
    void only_leaked_disks_are_deleted()
    {
        createAndDestroyNodegroup(AVAILABLE);

        // Multiple disks are returned in gcloud-compute-disks-list.json, but
        // only two of them were listed in kubectl-get-persistentvolumes.json
        assertThat(mockCommandExecutor.getCommandsExecuted().stream()
            .filter(command -> command.startsWith("gcloud compute disks delete")))
                .containsExactly(
                    "gcloud compute disks delete --project fake --quiet --zone us-west2-a gke-gke-elasticsearch--pvc-37dceae5-9a5e-4812-95d4-dc7751031ea2",
                    "gcloud compute disks delete --project fake --quiet --zone us-west2-a gke-gke-elasticsearch--pvc-cb130378-db0e-42bb-94a3-6f4f909fb34f");
    }

    @Test
    void only_disks_created_for_dynamic_pvs_are_deleted()
    {
        kubectlGetPersistentVolumesCommand
            .outputsOnStdout(kubectlGetPersistentVolumesOutputSecondDiskIsDynamicallyAllocated(false));

        createAndDestroyNodegroup(AVAILABLE);

        assertThat(mockCommandExecutor.getCommandsExecuted().stream()
            .filter(command -> command.startsWith("gcloud compute disks delete")))
                .containsExactly(
                    "gcloud compute disks delete --project fake --quiet --zone us-west2-a gke-gke-elasticsearch--pvc-37dceae5-9a5e-4812-95d4-dc7751031ea2");
    }

    @Test
    void an_empty_pv_list_does_not_cause_any_disk_deletions()
    {
        kubectlGetPersistentVolumesCommand
            .outputsOnStdout("""
                {
                    "apiVersion": "v1",
                    "items": [],
                    "kind": "List",
                    "metadata": {
                        "resourceVersion": "",
                        "selfLink": ""
                    }
                }""".stripIndent());

        createAndDestroyNodegroup(AVAILABLE);

        assertThat(mockCommandExecutor.getCommandsExecuted().stream()
            .filter(command -> command.startsWith("gcloud compute disks delete")))
                .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "[]"})
    void an_empty_disk_list_does_not_cause_any_disk_deletions(String output)
    {
        gcloudComputeDisksListCommand
            .outputsOnStdout(output);

        createAndDestroyNodegroup(AVAILABLE);

        assertThat(mockCommandExecutor.getCommandsExecuted().stream()
            .filter(command -> command.startsWith("gcloud compute disks delete")))
                .isEmpty();
    }

    static Stream<Arguments> getJsonCommandFailures(String validOutput, List<String> invalidEmptyOutputs)
    {
        return Stream.concat(
            Stream.of(
                // Fails with valid output
                Arguments.of(validOutput, 1),

                // Failures with invalid/empty output
                Arguments.of("{invalid json", 1),
                Arguments.of("", 1),
                Arguments.of("{}", 1),
                Arguments.of("[]", 1),

                // Successes with invalid output
                Arguments.of("{invalid json", 0)
            ),
            invalidEmptyOutputs.stream()
                .map(invalidEmptyOutput -> Arguments.of(invalidEmptyOutput, 0)));
    }

    public static Stream<Arguments> gcloudComputeDisksListFailures()
    {
        return getJsonCommandFailures(gcloudComputeDisksListOutput, List.of());
    }

    @ParameterizedTest
    @MethodSource("gcloudComputeDisksListFailures")
    void failing_to_collect_gcloud_disks_still_results_in_attempt_to_delete_them(String output, int exitCode)
    {
        gcloudComputeDisksListCommand
            .outputsOnStdout(output)
            .exitsWith(exitCode);

        createAndDestroyNodegroup(FAILED);

        assertThat(mockCommandExecutor.getCommandsExecuted().stream()
            .filter(command -> command.startsWith("gcloud compute disks delete")))
                .containsExactly(
                    "gcloud compute disks delete --project fake --quiet --zone us-west2-a gke-gke-elasticsearch--pvc-37dceae5-9a5e-4812-95d4-dc7751031ea2",
                    "gcloud compute disks delete --project fake --quiet --zone us-west2-a gke-gke-elasticsearch--pvc-cb130378-db0e-42bb-94a3-6f4f909fb34f");

        assertThat(loggedErrors)
            .anySatisfy(message -> assertThat(message).contains("attempting to delete all disks in cluster"));
    }

    static Stream<Arguments> kubeCtlGetPersistentVolumesFailures()
    {
        return getJsonCommandFailures(kubectlGetPersistentVolumesOutputSecondDiskIsDynamicallyAllocated(true),
            List.of("", "{}", "[]"));
    }

    @ParameterizedTest
    @MethodSource("kubeCtlGetPersistentVolumesFailures")
    void failing_to_collect_pvs_deletes_nothing_and_asks_user_to_manually_cleanup(String output, int exitCode)
    {
        kubectlGetPersistentVolumesCommand
            .outputsOnStdout(output)
            .exitsWith(exitCode);

        createAndDestroyNodegroup(FAILED);

        assertThat(mockCommandExecutor.getCommandsExecuted().stream()
            .filter(command -> command.startsWith("gcloud compute disks delete")))
                .isEmpty();

        assertThat(loggedErrors)
            .anySatisfy(message -> assertThat(message).contains("YOU MUST MANUALLY CHECK FOR LEAKED GCE DISKS"));
    }
}
