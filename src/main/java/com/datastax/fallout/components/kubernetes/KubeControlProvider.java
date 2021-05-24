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

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import org.apache.commons.io.FilenameUtils;

import com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.JsonUtils;
import com.datastax.fallout.util.NamedThreadFactory;
import com.datastax.fallout.util.ScopedLogger;

import static com.datastax.fallout.service.core.User.DockerRegistryCredential;

public class KubeControlProvider extends Provider
{
    private static final HashedWheelTimer timer =
        new HashedWheelTimer(new NamedThreadFactory("KubernetesReadyTimer"));
    private static final Duration podCreationTimeout = Duration.seconds(30);
    public static final Duration PORT_FORWARDING_WAIT = Duration.seconds(10);

    private final AbstractKubernetesProvisioner kubernetesProvisioner;
    private final Consumer<String> namespaceCreationCallback;

    public KubeControlProvider(Node node, AbstractKubernetesProvisioner kubernetesProvisioner,
        Consumer<String> namespaceCreationCallback)
    {
        super(node);
        this.kubernetesProvisioner = kubernetesProvisioner;
        this.namespaceCreationCallback = namespaceCreationCallback;
    }

    @Override
    public String name()
    {
        return "kube_control_provider";
    }

    public List<String> getNamespaces()
    {
        FullyBufferedNodeResponse getNamespaces = kubernetesProvisioner.executeInKubernetesEnv(
            "kubectl get namespaces -o=jsonpath='{$.items..metadata.name}'").buffered();
        if (!getNamespaces.waitForSuccess())
        {
            throw new RuntimeException("Could not get namespaces");
        }
        return Arrays.asList(getNamespaces.getStdout().split(" "));
    }

    public <T> T inNamespace(Optional<String> namespace, Function<NamespacedKubeCtl, T> operation)
    {
        return operation.apply(createNamespacedKubeCtl(namespace));
    }

    public NamespacedKubeCtl createNamespacedKubeCtl(Optional<String> namespace)
    {
        return new NamespacedKubeCtl(namespace);
    }

    @VisibleForTesting
    public static String kubectl(Optional<String> namespace, String command)
    {
        String cmdPrefix = "kubectl ";
        command = command.trim();

        if (namespace.isPresent())
        {
            // Saying `kubectl -n foo kots` produces an error as of kubectl 1.20.4 and kots 1.31.1: we need
            // to provide the namespace to the kots subcommand:
            final var KOTS_PREFIX = "kots ";
            if (command.startsWith(KOTS_PREFIX))
            {
                cmdPrefix += KOTS_PREFIX;
                command = command.substring(KOTS_PREFIX.length());
            }

            cmdPrefix += String.format("--namespace=%s ", namespace.get());
        }

        return cmdPrefix + command;
    }

    public JsonNode getPersistentVolumes()
    {
        FullyBufferedNodeResponse getPVs = kubernetesProvisioner.executeInKubernetesEnv(kubectl(Optional.empty(),
            "get persistentvolumes -o json")).buffered();
        if (!getPVs.waitForSuccess())
        {
            throw new RuntimeException("Error getting persistent volumes");
        }
        return JsonUtils.getJsonNode(getPVs.getStdout(), "/items");
    }

    public boolean clearPersistentVolumeClaimRef(String persistentVolumeName)
    {
        return kubernetesProvisioner.executeInKubernetesEnv(kubectl(Optional.empty(), String.format(
            "patch persistentvolume %s -p '{\"spec\":{\"claimRef\": null}}'", persistentVolumeName))).waitForSuccess();
    }

    public class NamespacedKubeCtl
    {
        private final Optional<String> namespace;

        NamespacedKubeCtl(Optional<String> namespace)
        {
            this.namespace = namespace;
            ensureNamespaceExists();
        }

        public NodeResponse execute(String command)
        {
            return kubernetesProvisioner.executeInKubernetesEnv(
                kubectl(namespace, command), logger());
        }

        boolean createNamespace()
        {
            return namespace.map(ns -> {
                String createNameSpaceCmd = String.format("kubectl create ns %s", ns);
                boolean result = kubernetesProvisioner.executeInKubernetesEnv(createNameSpaceCmd).waitForSuccess();
                namespaceCreationCallback.accept(ns);
                return result;
            })
                .orElse(true);
        }

        void ensureNamespaceExists()
        {
            namespace.ifPresent(ns -> {
                boolean namespaceExists = getNamespaces().contains(ns);
                if (!namespaceExists && !createNamespace())
                {
                    throw new RuntimeException("Could not ensure namespace exists");
                }
            });
        }

        public NodeResponse applyManifest(Path manifest)
        {
            return execute(String.format("apply -f %s", manifest.toAbsolutePath()));
        }

        public NodeResponse deleteResource(Path manifestArtifact)
        {
            return execute(String.format("delete -f %s", manifestArtifact));
        }

        public NodeResponse makeDirs(String containerName, String podName, String dir)
        {
            return execute(String.format("exec --container=%s --stdin=true %s -- mkdir -p %s",
                containerName, podName, dir));
        }

        public NodeResponse copyToContainer(String podName, String containerName, Path localPath,
            String pathInContainer)
        {
            return execute(String.format("cp --container=%s %s %s:%s",
                containerName, localPath, podName, pathInContainer));
        }

        public NodeResponse copyFromContainer(String podName, String containerName, String pathInContainer,
            Path localPath)
        {
            return execute(String.format("cp --container=%s %s:%s %s",
                containerName, podName, pathInContainer, localPath));
        }

        public boolean createDockerSecret(String secretName, DockerRegistryCredential cred)
        {
            return createDockerSecret(secretName, cred.dockerRegistry, cred.username, cred.password);
        }

        public boolean createDockerSecret(String secretName, String dockerServer, String username, String password)
        {
            return execute(String.format(
                "create secret docker-registry %s --docker-server='%s' --docker-username='%s' --docker-password='%s' --docker-email='dummy@fallout.com'",
                secretName, dockerServer, username, password))
                    .waitForSuccess();
        }

        public boolean deleteSecret(String secretName)
        {
            return execute(String.format("delete secret %s", secretName)).waitForSuccess();
        }

        public List<String> findPodNames()
        {
            return findPodNames(Optional.empty(), false);
        }

        public List<String> findPodNames(String labelSelector)
        {
            return findPodNames(labelSelector, false);
        }

        public List<String> findPodNames(String labelSelector, boolean restrictToNode)
        {
            return findPodNames(Optional.of(labelSelector), restrictToNode);
        }

        private List<String> findPodNames(Optional<String> labelSelector, boolean restrictToNode)
        {
            String filter = labelSelector
                .map(selector -> String.format("--selector %s", selector))
                .orElse("");

            if (restrictToNode)
            {
                filter += nodeNameFieldSelector();
            }

            FullyBufferedNodeResponse getPodNames = execute(String.format(
                "get pods -o=jsonpath='{$.items..metadata.name}' %s", filter)).buffered();
            if (!getPodNames.waitForSuccess())
            {
                throw new RuntimeException(String.format("Error while getting pods matching: %s", filter));
            }
            else if (getPodNames.getStdout().isEmpty())
            {
                logger().info("Found no pods matching: {}", filter);
                return List.of();
            }
            return List.of(getPodNames.getStdout().split(" "));
        }

        public Optional<JsonNode> getPodsJson(boolean restrictToNode)
        {
            String getPodsJsonCommand = "get pods -o json";
            if (restrictToNode)
            {
                getPodsJsonCommand += nodeNameFieldSelector();
            }
            FullyBufferedNodeResponse getPods = execute(getPodsJsonCommand).buffered();
            if (!getPods.waitForSuccess())
            {
                logger().warn("Could not get pods json");
                return Optional.empty();
            }
            return Optional.of(JsonUtils.getJsonNode(getPods.getStdout(), "/items"));
        }

        private String nodeNameFieldSelector()
        {
            return String.format(" --field-selector spec.nodeName=%s",
                node().getProvider(KubernetesNodeInfoProvider.class).getNodeName());
        }

        public List<String> findPodNamesRunningImage(String imageName)
        {
            return findPodNamesRunningImage(imageName, false);
        }

        public List<String> findPodNamesRunningImage(String imageName, boolean restrictToNode)
        {
            Optional<JsonNode> podsJson = getPodsJson(restrictToNode);
            if (podsJson.isEmpty())
            {
                logger().info("Found no pods running image {}", imageName);
                return List.of();
            }
            List<String> pods = new ArrayList<>();
            for (JsonNode pod : podsJson.get())
            {
                JsonNode containers = pod.at("/spec/containers");
                for (JsonNode container : containers)
                {
                    if (container.path("image").asText().contains(imageName))
                    {
                        pods.add(pod.at("/metadata/name").asText());
                        break;
                    }
                }
            }
            return pods;
        }

        public List<String> getAllContainersInPod(String pod)
        {
            FullyBufferedNodeResponse getPodContainers = execute(
                String.format("get pods %s -o=jsonpath='{.spec.containers[*].name}'", pod)).buffered();
            if (!getPodContainers.waitForSuccess())
            {
                throw new RuntimeException(String.format("Error while finding containers on pod %s", pod));
            }
            if (getPodContainers.getStdout().isEmpty())
            {
                logger().info("Found no containers on pod {}", pod);
                return List.of();
            }
            return List.of(getPodContainers.getStdout().split(" "));
        }

        public void captureContainerLogs(String pod, Optional<String> container, Path logArtifact)
        {
            captureContainerLogs(pod, container, logArtifact, false);
        }

        public void captureContainerLogs(String pod, Optional<String> container, Path logArtifact, boolean withPrevious)
        {
            if (withPrevious)
            {
                internalCaptureContainerLogs(pod, container, logArtifact.resolveSibling(
                    FilenameUtils.getBaseName(logArtifact.getFileName().toString()) +
                        ".previous." +
                        FilenameUtils.getExtension(logArtifact.getFileName().toString())),
                    true);
            }
            internalCaptureContainerLogs(pod, container, logArtifact, false);
        }

        private void internalCaptureContainerLogs(String pod, Optional<String> container, Path logArtifact,
            boolean previous)

        {
            String containerArg = container.map(container_ -> " " + container_).orElse("");
            FullyBufferedNodeResponse getContainerLog =
                execute(String.format("logs %s%s%s", previous ? "--previous " : "", pod, containerArg)).buffered();
            final var succeeded =
                previous ? getContainerLog.waitForOptionalSuccess() : getContainerLog.waitForSuccess();
            if (!succeeded)
            {
                if (previous)
                {
                    logger().info("Cannot capture logs for previous execution of pod " + pod + " (" + container + ")");
                    return;
                }
                else
                {
                    throw new RuntimeException(String.format("Error while getting log from pod %s container %s",
                        pod, container));
                }
            }
            if (getContainerLog.getStdout().isEmpty())
            {
                if (previous)
                {
                    logger().info("Empty previous execution logs for pod {}{}", pod,
                        container.map(container_ -> String.format(" container %s", container_)).orElse(""));
                }
                else
                {
                    logger().info("Empty log for pod {}{}", pod,
                        container.map(container_ -> String.format(" container %s", container_)).orElse(""));
                }
            }
            FileUtils.writeString(logArtifact, getContainerLog.getStdout());
        }

        public CompletableFuture<Boolean> waitUntilContainersReady(String containerName, int expectedContainers,
            Duration timeout, Duration pollInterval)
        {
            Utils.AwaitConditionOptions awaitOptions = new Utils.AwaitConditionOptions(node().logger(),
                () -> checkContainersReady(containerName, expectedContainers), timeout, timer, pollInterval);
            awaitOptions.addTestRunAbortTimeout(node());
            return Utils.awaitConditionAsync(awaitOptions);
        }

        public boolean checkContainersReady(String containerName, int expectedContainers)
        {
            String readyContainersJsonPath =
                String.format("{$.items..containerStatuses[?(@.name==\"%s\")].ready}", containerName);
            String getReadyContainersCmd = String.format("get pods -o=jsonpath='%s'", readyContainersJsonPath);
            FullyBufferedNodeResponse getReadyContainers = execute(getReadyContainersCmd).buffered();
            if (!getReadyContainers.waitForSuccess())
            {
                return false;
            }
            long readyContainers = Arrays.stream(getReadyContainers.getStdout().split(" "))
                .map(Boolean::valueOf)
                .filter(b -> b)
                .count();
            node().logger().info("Found {} of {} containers ready for {}", readyContainers, expectedContainers,
                containerName);
            if (expectedContainers == -1)
            {
                return readyContainers >= 1;
            }
            return readyContainers == expectedContainers;
        }

        public boolean waitForManifest(Path manifest, String manifestContent,
            KubernetesDeploymentManifestSpec.ManifestWaitOptions waitOptions)
        {
            String conditionAndTimeout = String.format("--for=%s --timeout=%ds", waitOptions.getCondition(),
                waitOptions.getTimeout().toSeconds());

            switch (waitOptions.getStrategy())
            {
                case WAIT_ON_CONTAINERS:
                    return waitUntilContainersReady(waitOptions.getContainerName(),
                        waitOptions.getExpectedContainers(manifestContent), waitOptions.getTimeout(),
                        Duration.minutes(1)).join();
                case WAIT_ON_MANIFEST:
                    String waitOnManifestCmd = String.format("wait %s -f %s", conditionAndTimeout,
                        manifest.toAbsolutePath());
                    return execute(waitOnManifestCmd).waitForSuccess();
                case WAIT_ON_PODS:
                    return waitOnPods(() -> findPodNames(waitOptions.getPodLabel()), conditionAndTimeout);
                case FIXED_DURATION:
                    Exceptions.runUninterruptibly(() -> Thread.sleep(waitOptions.getTimeout().toMillis()));
                    return true;
                case WAIT_ON_IMAGE:
                    return waitOnPods(() -> findPodNamesRunningImage(waitOptions.getImageName(manifestContent)),
                        conditionAndTimeout);
            }
            return false;
        }

        private boolean waitOnPods(Supplier<List<String>> podNameSupplier, String conditionAndTimeout)
        {
            Utils.AwaitConditionOptions podsAreCreated = new Utils.AwaitConditionOptions(logger(),
                () -> !podNameSupplier.get().isEmpty(), podCreationTimeout, timer, Duration.seconds(5));
            podsAreCreated.addTestRunAbortTimeout(node());

            if (!Utils.awaitConditionAsync(podsAreCreated).join())
            {
                logger().error("Pods were not created within {}", podCreationTimeout);
                return false;
            }

            String waitOnPodCmdBase = "wait %s pod %s";
            return Utils.waitForSuccess(logger(), podNameSupplier.get().stream()
                .map(n -> String.format(waitOnPodCmdBase, conditionAndTimeout, n))
                .map(this::execute)
                .collect(Collectors.toList()));
        }

        private String helmChartArguments(String name, String chartLocation, Map<String, String> flags,
            Optional<Path> optionsFile, boolean debug, Duration timeout, Optional<String> version)
        {
            StringBuilder arguments = new StringBuilder(String.format("%s --wait%s --timeout %ss %s %s",
                maybeWithHelmNamespace(), debug ? " --debug" : "", timeout.toSeconds(), name, chartLocation));
            version.ifPresent(v -> arguments.append(" --version=").append(v));
            flags.forEach((key, value) -> arguments.append(String.format(" --set %s=%s", key, value)));
            optionsFile.ifPresent(p -> arguments.append(String.format(" -f %s", p)));
            return arguments.toString();
        }

        private boolean successfulHelmCommand(NodeResponse command, Duration timeout)
        {
            return command.doWait().withTimeout(timeout).withDisabledTimeoutAfterNoOutput().forSuccess();
        }

        public boolean installHelmChart(Path helmChart, Map<String, String> flags, Optional<Path> optionsFile,
            boolean debug, Duration timeout, Optional<String> version)
        {
            return installHelmChart(helmChart.getFileName().toString(), helmChart.toAbsolutePath().toString(), flags,
                optionsFile, debug, timeout, version);
        }

        public boolean installHelmChart(String name, String chartLocation, Map<String, String> flags,
            Optional<Path> optionsFile, boolean debug, Duration timeout, Optional<String> version)
        {
            String install = String.format("helm install %s",
                helmChartArguments(name, chartLocation, flags, optionsFile, debug, timeout, version));
            return successfulHelmCommand(kubernetesProvisioner.executeInKubernetesEnv(install, logger()), timeout);
        }

        public boolean upgradeHelmChart(String name, String chartLocation, Map<String, String> flags,
            Optional<Path> optionsFile, boolean debug, Duration timeout, Optional<String> version)
        {
            String upgrade = String.format("helm upgrade --reuse-values %s",
                helmChartArguments(name, chartLocation, flags, optionsFile, debug, timeout, version));
            return successfulHelmCommand(kubernetesProvisioner.executeInKubernetesEnv(upgrade, logger()), timeout);
        }

        public boolean uninstallHelmChart(Path helmChart)
        {
            return kubernetesProvisioner
                .executeInKubernetesEnv(String.format("helm uninstall %s%s", helmChart.getFileName().toString(),
                    maybeWithHelmNamespace()))
                .waitForSuccess();
        }

        public boolean uninstallHelmChart(String installName)
        {
            return kubernetesProvisioner
                .executeInKubernetesEnv(String.format("helm uninstall %s%s", installName,
                    maybeWithHelmNamespace()))
                .waitForSuccess();
        }

        public boolean checkHelmChartDeployed(String releaseName)
        {
            String statusCommand = String.format("helm status %s%s -o json", releaseName, maybeWithHelmNamespace());
            FullyBufferedNodeResponse statusRes =
                kubernetesProvisioner.executeInKubernetesEnv(statusCommand).buffered();
            return statusRes.doWait().withNonZeroIsNoError().forSuccess() &&
                JsonUtils.getJsonNode(statusRes.getStdout(), "/info/status").asText().equals("deployed");
        }

        private String maybeWithHelmNamespace()
        {
            return namespace.map(ns -> String.format(" --namespace %s", ns)).orElse("");
        }

        public Optional<Boolean> executeIfNodeHasPod(String action, String labelSelector,
            BiFunction<NamespacedKubeCtl, String, Boolean> f)
        {
            return node().getProvider(KubeControlProvider.class).inNamespace(namespace, namespacedKubeCtl -> {
                List<String> podNames = namespacedKubeCtl.findPodNames(labelSelector, true);
                if (!podNames.isEmpty())
                {
                    return Optional.of(f.apply(namespacedKubeCtl, podNames.get(0)));
                }
                node().logger().info("No pod with label {} was found, will not {}", labelSelector, action);
                return Optional.empty();
            });
        }

        public boolean addHelmRepo(String name, String repoUrl)
        {
            String addRepoCommand = String.format("helm repo add %s %s", name, repoUrl);
            return kubernetesProvisioner.executeInKubernetesEnv(addRepoCommand).waitForSuccess();
        }

        public boolean createConfigMap(String name, Path sourceFile)
        {
            return execute(String.format("create configmap %s --from-file=%s", name, sourceFile.toAbsolutePath()))
                .waitForSuccess();
        }

        public boolean createConfigMap(String name, Map<String, String> data)
        {
            String literals = data.entrySet().stream()
                .map(e -> String.format("--from-literal=%s=%s", e.getKey(), e.getValue()))
                .collect(Collectors.joining(" "));
            return execute(String.format("create configmap %s %s", name, literals)).waitForSuccess();
        }

        Optional<JobStatus> getJobStatus(String jobName)
        {
            String rawJobStatus = getRawJobStatus(jobName);

            return KubeControlProvider.this.parseJobStatus(rawJobStatus);
        }

        String getRawJobStatus(String jobName)
        {
            FullyBufferedNodeResponse jobGet = execute(String.format("get job %s -o=json", jobName)).buffered();
            if (!jobGet.waitForSuccess())
            {
                throw new RuntimeException(String.format("Error getting job status for: %s", jobName));
            }
            return jobGet.getStdout();
        }

        public List<String> getJobPods(String jobName)
        {
            FullyBufferedNodeResponse getJobPods = execute(
                String.format("get pods --selector=job-name=%s -o=jsonpath={$.items..metadata.name}", jobName))
                    .buffered();
            if (!getJobPods.waitForSuccess())
            {
                throw new RuntimeException(String.format("Error finding pods associated with job: %s", jobName));
            }

            return List.of(getJobPods.getStdout().split(" "));
        }

        void cleanupJob(String jobName, Path jobManifest)
        {
            ScopedLogger scopedLogger = ScopedLogger.getLogger(logger());
            scopedLogger.withScopedInfo("Cleaning up job: {}", jobName).run(() -> {
                List<String> jobPods = getJobPods(jobName);
                boolean allPodsDeleted = scopedLogger.withScopedInfo("Deleting job pods: {}", jobPods)
                    .get(() -> Utils.waitForSuccess(logger(), jobPods.stream()
                        .map(jobPod -> execute(String.format("delete pod %s", jobPod)))
                        .collect(Collectors.toList())));

                scopedLogger.info("Deleting job: {}", jobName);
                boolean jobDeleted = deleteResource(jobManifest).waitForSuccess();

                if (!allPodsDeleted || !jobDeleted)
                {
                    throw new RuntimeException(
                        String.format("Could not cleanup all pods and job for job: %s", jobName));
                }
            });
        }

        public Boolean withPortForwarding(String serviceName, int k8sLocalPort,
            Consumer<Integer> doThingsWhilePortForwarded)
        {
            return Exceptions.getUncheckedIO(() -> {
                try (ServerSocket serverSocket = new ServerSocket(0))
                {
                    int falloutHostPort = serverSocket.getLocalPort();
                    NodeResponse portForwardAction = execute(
                        String.format("port-forward svc/%s %s:%s", serviceName, falloutHostPort, k8sLocalPort));

                    // wait for port-forward action
                    Exceptions.runUninterruptibly(() -> Thread.sleep(PORT_FORWARDING_WAIT.toMillis()));
                    logger().info("port-forward for svc/{} finished successfully", serviceName);

                    doThingsWhilePortForwarded.accept(falloutHostPort);
                    portForwardAction.kill();
                    return portForwardAction.doWait().withNonZeroIsNoError().forProcessEnd();
                }
            });
        }
    }

    static Optional<JobStatus> parseJobStatus(String rawJobStatus)
    {
        JsonNode jobStatus = JsonUtils.getJsonNode(rawJobStatus, "/status");

        // It is possible none of these keys are yet present in the Job's status.
        // Leave handling of this case to the caller.
        if (jobStatus.has("active"))
        {
            return Optional.of(JobStatus.ACTIVE);
        }
        else if (jobStatus.has("failed"))
        {
            return Optional.of(JobStatus.FAILED);
        }
        else if (jobStatus.has("succeeded"))
        {
            return Optional.of(JobStatus.SUCCEEDED);
        }
        return Optional.empty();
    }

    public enum JobStatus
    {
        ACTIVE,
        SUCCEEDED,
        FAILED
    }
}
