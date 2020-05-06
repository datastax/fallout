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
package com.datastax.fallout.ops.providers;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.util.HashedWheelTimer;

import com.datastax.fallout.harness.specs.KubernetesManifestSpec;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.provisioner.kubernetes.AbstractKubernetesProvisioner;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.NamedThreadFactory;

public class KubeControlProvider extends Provider
{
    private static final HashedWheelTimer timer =
        new HashedWheelTimer(new NamedThreadFactory("KubernetesReadyTimer"));
    private static final Duration podCreationTimeout = Duration.seconds(30);

    private final AbstractKubernetesProvisioner kubernetesProvisioner;

    public KubeControlProvider(Node node, AbstractKubernetesProvisioner kubernetesProvisioner)
    {
        super(node);
        this.kubernetesProvisioner = kubernetesProvisioner;
    }

    @Override
    public String name()
    {
        return "kube_control_provider";
    }

    public <T> T inNamespace(Optional<String> namespace, Function<NamespacedKubeCtl, T> operation)
    {
        return operation.apply(new NamespacedKubeCtl(namespace));
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
            String fullCmd = String.format("%s %s", getCommandPrefix(), command);
            return kubernetesProvisioner.executeInKubernetesEnv(fullCmd, logger());
        }

        private String getCommandPrefix()
        {
            String cmdPrefix = "kubectl";
            if (namespace.isPresent())
            {
                cmdPrefix += String.format(" -n %s", namespace.get());
            }
            return cmdPrefix;
        }

        public boolean createNamespace()
        {
            return namespace.map(ns -> {
                String createNameSpaceCmd = String.format("kubectl create ns %s", ns);
                return kubernetesProvisioner.executeInKubernetesEnv(createNameSpaceCmd)
                    .doWait().withNonZeroIsNoError().forSuccess();
            })
                .orElse(true);
        }

        public void ensureNamespaceExists()
        {
            namespace.ifPresent(ns -> {
                FullyBufferedNodeResponse getNamespaces =
                    kubernetesProvisioner.executeInKubernetesEnv("kubectl get namespaces").buffered();
                getNamespaces.waitForSuccess();
                boolean namespaceExists = getNamespaces.getStdout().contains(ns);
                if (!namespaceExists)
                {
                    if (!createNamespace())
                    {
                        throw new RuntimeException("Could not ensure namespace exists");
                    }
                }
            });
        }

        public NodeResponse applyManifest(Path manifest)
        {
            return execute(String.format("apply -f %s", manifest.toAbsolutePath()));
        }

        public NodeResponse deleteResource(Path manifestArtifact)
        {
            return execute(String.format("delete --wait=false -f %s", manifestArtifact));
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

        public NodeResponse createSecret(String secretName, String dockerServer, String username, String password)
        {
            return execute(String.format(
                "create secret docker-registry %s --docker-server=%s --docker-username=%s --docker-password=%s",
                secretName, dockerServer, username, password));
        }

        public List<String> findPodNames(String labelSelector)
        {
            return findPodNames(labelSelector, false);
        }

        public List<String> findPodNames(String labelSelector, boolean restrictToNode)
        {
            String filter = String.format("--selector %s", labelSelector);
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
            return Arrays.asList(getPodNames.getStdout().split(" "));
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
            return Optional.of(Utils.getJsonNode(getPods.getStdout(), "/items"));
        }

        private String nodeNameFieldSelector()
        {
            return String.format(" --field-selector spec.nodeName=%s",
                node.getProvider(KubernetesNodeInfoProvider.class).getNodeName());
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
                    if (container.get("image").asText().contains(imageName))
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
            return Arrays.asList(getPodContainers.getStdout().split(" "));
        }

        public void captureContainerLogs(String pod, Optional<String> container, Path logArtifact)
        {
            String containerArg = container.map(container_ -> " " + container_).orElse("");
            FullyBufferedNodeResponse getContainerLog =
                execute(String.format("logs %s%s", pod, containerArg)).buffered();
            if (!getContainerLog.waitForSuccess())
            {
                throw new RuntimeException(String.format("Error while getting log from pod %s container %s",
                    pod, container));
            }
            if (getContainerLog.getStdout().isEmpty())
            {
                logger().info("Empty log for pod {}{}", pod,
                    container.map(container_ -> String.format(" container %s", container_)).orElse(""));
            }
            Utils.writeStringToFile(logArtifact.toFile(), getContainerLog.getStdout());
        }

        public CompletableFuture<Boolean> waitUntilContainersReady(String containerName, int expectedContainers)
        {
            return waitUntilContainersReady(containerName, expectedContainers, Duration.minutes(15),
                Duration.minutes(1));
        }

        public CompletableFuture<Boolean> waitUntilContainersReady(String containerName, int expectedContainers,
            Duration timeout, Duration pollInterval)
        {
            Utils.AwaitConditionOptions awaitOptions = new Utils.AwaitConditionOptions(node.logger(),
                () -> checkContainersReady(containerName, expectedContainers), timeout, timer, pollInterval);
            awaitOptions.addTestRunAbortTimeout(node);
            return Utils.awaitConditionAsync(awaitOptions);
        }

        public boolean checkAtLeastOneContainerIsReady(String containerName)
        {
            return checkContainersReady(containerName, -1);
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
            node.logger().info("Found {} of {} containers ready for {}", readyContainers, expectedContainers,
                containerName);
            if (expectedContainers == -1)
            {
                return readyContainers >= 1;
            }
            return readyContainers == expectedContainers;
        }

        public boolean waitForManifest(Path manifest, String manifestContent,
            KubernetesManifestSpec.ManifestWaitOptions waitOptions)
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
            podsAreCreated.addTestRunAbortTimeout(node);

            if (!Utils.awaitConditionAsync(podsAreCreated).join())
            {
                logger().error("Pods were not created within {}", podCreationTimeout);
                return false;
            }

            String waitOnPodCmdBase = "wait %s pod %s && exit $?";
            return Utils.waitForSuccess(logger(), podNameSupplier.get().stream()
                .map(n -> String.format(waitOnPodCmdBase, conditionAndTimeout, n))
                .map(this::execute)
                .collect(Collectors.toList()));
        }

        public boolean installHelmChart(Path helmChart, Map<String, String> flags, Optional<Path> optionsFile)
        {
            String name = helmChart.getFileName().toString();
            StringBuilder install = new StringBuilder(String.format("helm install %s %s", name, helmChart));
            namespace.ifPresent(ns -> install.append(" --namespace=").append(ns));
            flags.forEach((key, value) -> install.append(String.format(" --set %s=%s", key, value)));
            optionsFile.ifPresent(p -> install.append(String.format(" -f %s", p)));
            return kubernetesProvisioner.executeInKubernetesEnv(install.toString(), logger()).waitForSuccess();
        }

        public boolean uninstallHelmChart(Path helmChart)
        {
            return kubernetesProvisioner
                .executeInKubernetesEnv(String.format("helm uninstall %s%s", helmChart.getFileName().toString(),
                    namespace.map(ns -> String.format(" --namespace %s", ns)).orElse("")))
                .waitForSuccess();
        }

        public Optional<Boolean> executeIfNodeHasPod(String action, String labelSelector,
            BiFunction<NamespacedKubeCtl, String, Boolean> f)
        {
            return node.getProvider(KubeControlProvider.class).inNamespace(namespace, namespacedKubeCtl -> {
                List<String> podNames = namespacedKubeCtl.findPodNames(labelSelector, true);
                if (!podNames.isEmpty())
                {
                    return Optional.of(f.apply(namespacedKubeCtl, podNames.get(0)));
                }
                node.logger().info("No pod with label {} was found, will not {}", labelSelector, action);
                return Optional.empty();
            });
        }
    }
}
