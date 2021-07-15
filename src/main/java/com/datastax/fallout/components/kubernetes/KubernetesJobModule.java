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
package com.datastax.fallout.components.kubernetes;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.YamlUtils;

import static com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec.buildNameSpaceSpec;
import static com.datastax.fallout.components.kubernetes.KubeControlProvider.JobStatus;
import static com.datastax.fallout.components.kubernetes.KubeControlProvider.parseJobStatus;

@AutoService(Module.class)
public class KubernetesJobModule extends Module
{
    private static final String PREFIX = "fallout.modules.kubernetes.job.";

    private static final KubernetesManifestSpec manifestSpec = new KubernetesManifestSpec(PREFIX);
    private static final PropertySpec<String> namespaceSpec = buildNameSpaceSpec(PREFIX);

    private static final PropertySpec<String> targetGroupSpec =
        PropertySpecBuilder.nodeGroup(PREFIX, "target_group", "NodeGroup to deploy the job manifest on.", "server");

    private static final PropertySpec<Duration> timeoutSpec = PropertySpecBuilder.createDuration(PREFIX)
        .name("timeout")
        .description("Maximum amount of time to wait for job to successfully complete.")
        .defaultOf(Duration.minutes(30))
        .build();

    private static final PropertySpec<Boolean> captureContainerLogsSpec = PropertySpecBuilder.createBool(PREFIX)
        .name("capture_container_logs")
        .description("Capture the container logs belonging to the job")
        .defaultOf(true)
        .build();

    private static final PropertySpec<Boolean> capturePreviousContainerLogsSpec = PropertySpecBuilder.createBool(PREFIX)
        .name("capture_previous_container_logs")
        .description("Capture the previous container logs belonging to the job (kubectl logs --previous)")
        .defaultOf(false)
        .build();

    @Override
    public String prefix()
    {
        return PREFIX;
    }

    @Override
    public String name()
    {
        return "kubernetes_job";
    }

    @Override
    public String description()
    {
        return "Deploys the Kubernetes job in the given manifest, waits for the job to complete, cleans up the Job and associated pods, and finally checks it was successful.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("kubernetes/kubernetes-job.yaml");
    }

    @Override
    protected List<PropertySpec<?>> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .addAll(manifestSpec.getPropertySpecs())
            .add(namespaceSpec, targetGroupSpec, timeoutSpec)
            .add(captureContainerLogsSpec)
            .add(capturePreviousContainerLogsSpec)
            .build();
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        validator.nodeGroupRequiresProvider(targetGroupSpec, KubeControlProvider.class);
    }

    private NodeGroup targetGroup;
    private Optional<String> namespace;
    private Duration timeout;
    private String jobName;

    @SuppressWarnings("unchecked")
    private String extractJobName(NodeGroup targetGroup, PropertyGroup properties)
    {
        String yamlMultiDoc = manifestSpec.getManifestContent(targetGroup, properties);
        List<String> jobNames = YamlUtils.loadYamlDocuments(yamlMultiDoc).stream()
            .filter(jsonNode -> jsonNode.path("kind").asText().equals("Job"))
            .map(jsonNode -> jsonNode.at("/metadata/name").asText())
            .collect(Collectors.toList());

        if (jobNames.isEmpty())
        {
            throw new InvalidConfigurationException(
                "Manifest submitted to kubernetes_job module contains no kubernetes job");
        }

        if (jobNames.size() > 1)
        {
            throw new InvalidConfigurationException(
                "Found multiple kubernetes jobs, but the kubernetes_job module can only support one");
        }

        return jobNames.get(0);
    }

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        targetGroup = ensemble.getNodeGroupByAlias(targetGroupSpec.value(properties));
        namespace = namespaceSpec.optionalValue(properties);
        timeout = timeoutSpec.value(properties);
        jobName = extractJobName(targetGroup, properties);
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        emitInvoke("Starting kubernetes job");
        boolean jobResult = targetGroup.findFirstRequiredProvider(KubeControlProvider.class).inNamespace(namespace,
            kubeCtl -> handleKubernetesJob(kubeCtl, properties));

        if (jobResult)
        {
            emitOk("Kubernetes job was successful");
        }
        else
        {
            emitFail("Kubernetes job failed");
        }
    }

    private boolean handleKubernetesJob(KubeControlProvider.NamespacedKubeCtl kubeCtl, PropertyGroup properties)
    {
        Path manifest = manifestSpec.getManifestArtifactPath(targetGroup, properties);
        boolean jobApplied = kubeCtl.applyManifest(manifest).waitForSuccess();

        if (!jobApplied)
        {
            logger().error("Could not apply job manifest");
            return false;
        }
        Utils.AwaitConditionOptions await = new Utils.AwaitConditionOptions(logger(),
            () -> kubeCtl.getJobStatus(jobName).map(jobStatus -> jobStatus != JobStatus.ACTIVE).orElse(false),
            timeout, timer, Duration.minutes(1));
        boolean waitForJobCompletion = Utils.awaitConditionAsync(await).join();
        if (!waitForJobCompletion)
        {
            logger().error("Job did not complete within the given timeout");
        }
        String rawJobStatus = kubeCtl.getRawJobStatus(jobName);
        logger().info("Final job status {}", rawJobStatus);
        Optional<JobStatus> finalJobStatus = parseJobStatus(rawJobStatus);

        boolean success = waitForJobCompletion &&
            finalJobStatus.map(jobStatus -> jobStatus == JobStatus.SUCCEEDED).orElse(false);

        if (captureContainerLogsSpec.value(properties))
        {
            boolean capturePrevious = capturePreviousContainerLogsSpec.value(properties);
            Path targetDir = targetGroup.getLocalArtifactPath().resolve("job_logs");
            FileUtils.createDirs(targetDir);

            List<String> podNames = kubeCtl.getJobPods(jobName);
            for (String podName : podNames)
            {
                try
                {
                    List<String> allContainersInPod = kubeCtl.getAllContainersInPod(podName);
                    for (String container : allContainersInPod)
                    {
                        try
                        {
                            kubeCtl.captureContainerLogs(podName, Optional.of(container),
                                targetDir.resolve("pod-" + podName + "-" + container + ".logs"),
                                capturePrevious);
                        }
                        catch (RuntimeException err)
                        {
                            logger().error("Cannot get logs for pod {} container {}", podName, container);
                            success = false;
                        }
                    }
                }
                catch (RuntimeException err)
                {
                    logger().error("Cannot get logs for pod {}", podName, err);
                    success = false;
                }
            }
        }
        kubeCtl.cleanupJob(jobName, manifest);

        return success;
    }
}
