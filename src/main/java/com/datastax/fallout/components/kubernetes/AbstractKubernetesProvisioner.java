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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.provisioner.NoRemoteAccessProvisioner;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.JsonUtils;
import com.datastax.fallout.util.ResourceUtils;

import static com.datastax.fallout.util.MustacheFactoryWithoutHTMLEscaping.renderWithScopes;

public abstract class AbstractKubernetesProvisioner extends NoRemoteAccessProvisioner
{
    // Kubernetes labels must match. See https://kubernetes.io/docs/concepts/overview/working-with-objects/names/
    public static final Pattern DNS1123 =
        Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*");
    private static final String KUBECONFIG_ENV_VAR = "KUBECONFIG";
    private static final String FALLOUT_STORAGE_CLASS_NAME = "fallout-storage";
    private static final String ARTIFACT_COLLECTOR_CONTAINER_NAME = "artifact-collector";
    private Path kubeConfigPath;

    public AbstractKubernetesProvisioner()
    {
        super("k8s");
    }

    public NodeResponse executeInKubernetesEnv(String command)
    {
        return executeInKubernetesEnv(command, getNodeGroup().logger());
    }

    public NodeResponse executeInKubernetesEnv(String command, Logger logger)
    {
        return getCommandExecutor().local(logger, command).environment(getKubernetesEnv()).execute();
    }

    final Path getKubeConfigPath()
    {
        Preconditions.checkState(getOptionalKubeConfigPath().isPresent());
        return getOptionalKubeConfigPath().get();
    }

    Optional<Path> getOptionalKubeConfigPath()
    {
        if (kubeConfigPath == null)
        {
            kubeConfigPath = getNodeGroup().getLocalArtifactPath().resolve("kube-config.yaml");
        }
        return Optional.of(kubeConfigPath);
    }

    public Map<String, String> getKubernetesEnv()
    {
        final var env = ImmutableMap.<String, String>builder();

        getOptionalKubeConfigPath().ifPresent(path -> {
            env.put(KUBECONFIG_ENV_VAR, path.toString());
        });

        final var helmScratch =
            getNodeGroup().getLocalScratchSpace().makeScratchSpaceFor(this).makeScratchSpaceFor("helm");

        env.put("HELM_CACHE_HOME", helmScratch.makeScratchSpaceFor("cache").getPath().toString());
        env.put("HELM_CONFIG_HOME", helmScratch.makeScratchSpaceFor("config").getPath().toString());
        env.put("HELM_DATA_HOME", helmScratch.makeScratchSpaceFor("data").getPath().toString());

        return env.build();
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(KubeControlProvider.class);
    }

    @Override
    protected CheckResourcesResult createImpl(NodeGroup nodeGroup)
    {
        if (!createKubernetesCluster(nodeGroup))
        {
            return CheckResourcesResult.FAILED;
        }
        return CheckResourcesResult.fromWasSuccessful(postCreate(nodeGroup));
    }

    protected abstract boolean createKubernetesCluster(NodeGroup nodeGroup);

    protected void onNamespaceCreation(String namespace)
    {
    }

    protected int getEffectiveNodeCount(NodeGroup nodeGroup, JsonNode nodesList)
    {
        Preconditions.checkArgument(nodesList.size() == nodeGroup.getNodes().size(),
            "Found unexpected number of kubernetes nodes. Nodegroup declared %s nodes, but found %s",
            nodeGroup.getNodes().size(), nodesList.size());
        return nodesList.size();
    }

    /** If this returns non-empty, it must be the path to a kubernetes storage class manifest that creates a
     *  storage class named {@link FALLOUT_STORAGE_CLASS_NAME}; this will then be applied to the cluster.
     *
     *  When {@link #downloadProvisionerArtifactsImpl} runs, it will download the
     *  contents of all persistent volumes with the {@link FALLOUT_STORAGE_CLASS_NAME}. */
    protected Optional<Path> getProvisionerStorageClassYaml()
    {
        return Optional.empty();
    }

    private boolean deployFalloutStorageClass()
    {
        return getProvisionerStorageClassYaml()
            .map(storageClassYaml -> executeInKubernetesEnv(
                String.format("kubectl apply -f %s", storageClassYaml)).waitForSuccess())
            .orElse(true);
    }

    boolean postCreate(NodeGroup nodeGroup)
    {
        if (!postCreateImpl(nodeGroup))
        {
            return false;
        }

        nodeGroup.getNodes().forEach(n -> new KubeControlProvider(n, this, this::onNamespaceCreation));

        String nodeInfoCmd = "kubectl get nodes -o json";
        FullyBufferedNodeResponse nodeInfo = executeInKubernetesEnv(nodeInfoCmd).buffered();
        if (!nodeInfo.waitForSuccess())
        {
            return false;
        }

        JsonNode items = JsonUtils.getJsonNode(nodeInfo.getStdout(), "/items");
        createKubernetesNodeInfoProviders(nodeGroup, items);

        return deployFalloutStorageClass();
    }

    protected void createKubernetesNodeInfoProviders(NodeGroup nodeGroup, JsonNode nodesList)
    {
        for (int i = 0; i < getEffectiveNodeCount(nodeGroup, nodesList); i++)
        {
            Node node = nodeGroup.getNodes().get(i);
            JsonNode nodeInfo = nodesList.get(i);
            Pair<String, String> ips = extractIpAddressesFromKubectlJson(nodeInfo);
            new KubernetesNodeInfoProvider(node, ips.getLeft(), ips.getRight(),
                extractNodeNameFromKubectlJson(nodeInfo));
        }
    }

    protected String extractNodeNameFromKubectlJson(JsonNode nodeInfo)
    {
        return nodeInfo.at("/metadata/name").asText();
    }

    protected Pair<String, String> extractIpAddressesFromKubectlJson(JsonNode nodeInfo)
    {
        JsonNode addresses = nodeInfo.at("/status/addresses");
        String internal = null;
        String external = null;
        for (JsonNode address : addresses)
        {
            String type = address.get("type").asText();
            String val = address.get("address").asText();
            switch (type)
            {
                case "InternalIP":
                    internal = val;
                    break;
                case "ExternalIP":
                    external = val;
                    break;
            }
            if (external == null)
            {
                external = internal;
            }
        }
        return Pair.of(external, internal);
    }

    protected boolean postCreateImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected boolean destroyImpl(NodeGroup nodeGroup)
    {
        nodeGroup.maybeUnregisterProviders(KubeControlProvider.class);
        return destroyKubernetesCluster(nodeGroup);
    }

    protected abstract boolean destroyKubernetesCluster(NodeGroup nodeGroup);

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        NodeGroup.State existingClusterState = checkExistingKubernetesClusterState(nodeGroup);
        if (existingClusterState == NodeGroup.State.FAILED || existingClusterState == NodeGroup.State.DESTROYED)
        {
            return existingClusterState;
        }
        if (postCreate(nodeGroup))
        {
            return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        }
        return NodeGroup.State.FAILED;
    }

    protected abstract NodeGroup.State checkExistingKubernetesClusterState(NodeGroup nodeGroup);

    Path writeRenderedDefinition(Path dest, String template, Map<String, Object> scope)
    {
        String definition = ResourceUtils.getResourceAsString(getClass(), template);
        FileUtils.writeString(dest, renderWithScopes(definition, List.of(scope)));
        return dest;
    }

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected boolean stopImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    /**
     * Not all sub-classes deploy a storage class with the {@link FALLOUT_STORAGE_CLASS_NAME}. Further,
     * not all kubernetes based tests have the required permissions to execute {@link collectPersistentVolumeArtifacts}.
     * Currently we only run PV artifact collection when Fallout has deployed a storage class, and assume the
     * necessary permissions are in place.
     */
    @Override
    protected boolean downloadProvisionerArtifactsImpl(NodeGroup nodeGroup, Path localPath)
    {
        return getProvisionerStorageClassYaml()
            .map(ignored -> collectPersistentVolumeArtifacts(nodeGroup))
            .orElse(true);
    }

    private boolean deployPersistentVolumeClaimForArtifactCollector(NodeGroup nodeGroup, Path manifestScratchSpace,
        JsonNode pv, String pvName, String pvcName, KubeControlProvider kubeCtl)
    {
        String pvcTemplate = ResourceUtils.getResourceAsString(getClass(), "artifact-collector-pvc.yaml");
        String pvcDefinition = renderWithScopes(pvcTemplate, List.of(Map.of(
            "pvc-name", pvcName,
            "pv-name", pvName,
            "pv-capacity", pv.at("/spec/capacity/storage").asText())));
        Path pvcManifest = writeRenderedDefinition(manifestScratchSpace.resolve(String.format("%s-pvc.yaml", pvName)),
            "artifact-collector-pvc.yaml",
            Map.of("pvc-name", pvcName, "pv-name", pvName, "pv-capacity", pv.at("/spec/capacity/storage").asText()));
        FileUtils.writeString(pvcManifest, pvcDefinition);
        boolean deployed = kubeCtl.inNamespace(Optional.empty(),
            namespacedKubectl -> namespacedKubectl.applyManifest(pvcManifest).waitForSuccess());
        if (!deployed)
        {
            nodeGroup.logger().error("Failed to deploy PersistentVolumeClaim for PV: {}", pvName);
        }
        return deployed;
    }

    private CompletableFuture<Boolean> deployArtifactCollectorPod(NodeGroup nodeGroup, Path manifestScratchSpace,
        Map<String, String> targetPv, KubeControlProvider kubeCtl)
    {
        return CompletableFuture.supplyAsync(() -> {
            String pvName = targetPv.get("pv-name");
            Map<String, Object> templateValues = new HashMap<>(targetPv);
            templateValues.put("container-name", ARTIFACT_COLLECTOR_CONTAINER_NAME);
            Path manifest = writeRenderedDefinition(manifestScratchSpace
                .resolve(String.format("%s-collection.yaml", pvName)), "artifact-collector-pod.yaml", templateValues);
            boolean deployed = kubeCtl.inNamespace(Optional.empty(),
                namespacedKubectl -> namespacedKubectl.applyManifest(manifest).waitForSuccess());

            if (!deployed)
            {
                nodeGroup.logger().error("Error deploying persistent volume collector pod for pv: {}", pvName);
            }
            return deployed;
        }).exceptionally(t -> {
            nodeGroup.logger().error(
                String.format(
                    "Exception thrown while deploying artifact collector pod for persistent volume: %s",
                    targetPv.get("pv-name")),
                t);
            return false;
        });
    }

    /**
     * Collects artifacts from all live Persistent Volumes created with the fallout-storage storage class.
     */
    private boolean collectPersistentVolumeArtifacts(NodeGroup nodeGroup)
    {
        Path manifestScratchSpace = nodeGroup.getLocalScratchSpace()
            .makeScratchSpaceFor(this)
            .makeScratchSpaceFor("artifact-collection-manifests")
            .getPath();

        KubeControlProvider kubeCtl = nodeGroup.findFirstRequiredProvider(KubeControlProvider.class);

        JsonNode pvList = kubeCtl.getPersistentVolumes();
        List<Map<String, String>> pvInfos = new ArrayList<>();

        final AtomicBoolean success = new AtomicBoolean(true);

        for (var pv : pvList)
        {
            if (!pv.at("/spec/storageClassName").asText().equals(FALLOUT_STORAGE_CLASS_NAME))
            {
                continue;
            }

            String pvName = pv.at("/metadata/name").asText();
            String status = pv.at("/status/phase").asText();
            String pvcName;
            switch (status)
            {
                case "Terminating":
                    continue;
                case "Bound":
                    // A Persistent Volume can only be bound by a single Persistent Volume Claim.
                    // The artifact collector pod can reuse the existing PVC.
                    pvcName = pv.at("/spec/claimRef/name").asText();
                    break;
                case "Released":
                    // The Persistent Volume is no longer in use, but cannot be bound to a new Persistent Volume Claim
                    // until it's current claim ref is cleared.
                    kubeCtl.clearPersistentVolumeClaimRef(pvName);
                    // FALLTHROUGH
                case "Available":
                    pvcName = String.format("artifact-collection-pvc-%s", pvName);
                    if (!deployPersistentVolumeClaimForArtifactCollector(nodeGroup, manifestScratchSpace, pv, pvName,
                        pvcName, kubeCtl))
                    {
                        success.set(false);
                    }
                    break;
                default:
                    nodeGroup.logger().error("Found unexpected PersistentVolume status: {}", status);
                    success.set(false);
                    continue;
            }

            pvInfos.add(Map.of(
                "pv-name", pvName,
                "pvc-name", pvcName));
        }

        if (pvInfos.isEmpty())
        {
            nodeGroup.logger().info("Found no persistent volumes from which to collect artifacts");
            return success.get();
        }

        List<CompletableFuture<Boolean>> artifactCollectorDeploys = pvInfos.stream()
            .map(targetPv -> deployArtifactCollectorPod(
                nodeGroup, manifestScratchSpace, targetPv, kubeCtl))
            .toList();

        if (!Utils.waitForAll(artifactCollectorDeploys, nodeGroup.logger(), "Deploy artifact collector pods"))
        {
            nodeGroup.logger().error(
                "Error while applying artifact collector pods, attempting to collect artifacts where possible");
        }

        int numCollectors = (int) artifactCollectorDeploys.stream()
            .map(CompletableFuture::join)
            .filter(deployed -> deployed)
            .count();

        boolean collectorsReady = kubeCtl.inNamespace(Optional.empty(),
            namespacedKubectl -> namespacedKubectl.waitUntilContainersReady(
                ARTIFACT_COLLECTOR_CONTAINER_NAME, numCollectors, Duration.minutes(5), Duration.seconds(10)))
            .join();

        if (!collectorsReady)
        {
            nodeGroup.logger().error(
                "Some artifact collector pods failed to deploy, attempting to collect artifacts where possible");
        }

        List<NodeResponse> collections = kubeCtl.inNamespace(Optional.empty(), namespacedKubectl -> {
            List<String> collectorPods = namespacedKubectl.findPodNames(
                "app=fallout-artifact-collection --field-selector status.phase=Running");
            return collectorPods.stream()
                .map(pod -> {
                    Path pvArtifacts = nodeGroup.getLocalArtifactPath().resolve(String.format("%s-artifacts", pod));
                    return namespacedKubectl
                        .copyFromContainer(pod, ARTIFACT_COLLECTOR_CONTAINER_NAME, "/fallout-artifacts", pvArtifacts);
                })
                .toList();
        });

        if (!Utils.waitForSuccess(nodeGroup.logger(), collections, Duration.minutes(30)))
        {
            nodeGroup.logger().error("Error collecting artifacts from fallout-storage persistent volumes");
            success.set(false);
        }

        List<NodeResponse> collectorDeletes = kubeCtl.inNamespace(Optional.empty(),
            namespacedKubectl -> FileUtils.listDir(manifestScratchSpace).stream()
                .map(namespacedKubectl::deleteResource)
                .toList());

        if (!Utils.waitForSuccess(nodeGroup.logger(), collectorDeletes, Duration.minutes(5)))
        {
            nodeGroup.logger().error("Error deleting artifact collector pods and pvc");
            success.set(false);
        }

        return success.get();
    }
}
