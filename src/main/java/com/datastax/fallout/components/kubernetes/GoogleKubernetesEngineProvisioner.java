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

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auto.service.AutoService;
import com.google.cloud.logging.v2.LoggingClient;
import com.google.cloud.logging.v2.LoggingSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import com.google.logging.v2.ListLogEntriesRequest;
import com.google.logging.v2.LogEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.ops.ClusterNames;
import com.datastax.fallout.ops.FalloutPropertySpecs;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.ResourceType;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.DateUtils;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.JsonUtils;
import com.datastax.fallout.util.ResourceUtils;
import com.datastax.fallout.util.ScopedLogger;

import static com.datastax.fallout.harness.ActiveTestRunBuilder.NODE_COUNT_KEY;

@AutoService(Provisioner.class)
public class GoogleKubernetesEngineProvisioner extends AbstractKubernetesProvisioner
{
    // Limits the logging requests to 1 every 2 seconds. Argument must be converted to permits per second.
    private static final RateLimiter gcloudLoggingApiRateLimiter = RateLimiter.create(0.5);

    private static final String prefix = "fallout.provisioner.kubernetes.google.";
    private static final String CLOUD_SDK_CONFIG_ENV_VAR = "CLOUDSDK_CONFIG";
    private static final int DEFAULT_ZONES_PER_REGION = 3;
    private Path cloudSdkConfigPath;

    private final Set<String> userNamespaces = new HashSet<>();

    private static final PropertySpec<String> projectSpec = PropertySpecBuilder.createStr(prefix)
        .name("project")
        .description("Google Cloud Project to create the cluster within.")
        .required()
        .build();

    private static final PropertySpec<String> regionSpec = PropertySpecBuilder.createStr(prefix)
        .name("region")
        .description("Region in which to provision the kubernetes cluster.")
        .build();

    private static final PropertySpec<String> zoneSpec = PropertySpecBuilder.createStr(prefix)
        .name("zone")
        .description("The zone in which to provision all nodes.")
        .build();

    private static final PropertySpec<String> machineTypeSpec = PropertySpecBuilder.createStr(prefix)
        .name("machine.type")
        .description("Machine type of the nodes provisioned in the kubernetes cluster. See " +
            "<a>https://cloud.google.com/compute/docs/machine-types</a> for a complete list of valid machine types.")
        .required()
        .build();

    private static final PropertySpec<String> gkeMasterVersionSpec = PropertySpecBuilder.createStr(prefix)
        .name("cluster_version")
        .description("GKE version of the cluster.")
        .build();

    private static final PropertySpec<String> extraCreateArgsSpec = PropertySpecBuilder.createStr(prefix)
        .name("create.extra_args")
        .description("Any extra arguments which should be passed to the create command.")
        .build();

    public static final PropertySpec<String> serviceAccountSpec = PropertySpecBuilder.createStr(prefix)
        .name("gcloud.service_account")
        .description("Email of the service account which will execute all gcloud commands. Defaults to user profile.")
        .build();

    private PropertySpec<Boolean> collectGkeLogsSpec = PropertySpecBuilder.createBool(prefix, true)
        .name("collect_logs")
        .description("If true, use the GKE Logging service to collect logs from all namespaces during tear down.")
        .build();

    private Instant clusterCreationInstant;

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "gke";
    }

    @Override
    public String description()
    {
        return "Provisioner for the Google Kubernetes Engine.";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(projectSpec, regionSpec, zoneSpec, machineTypeSpec, gkeMasterVersionSpec, extraCreateArgsSpec,
                serviceAccountSpec, collectGkeLogsSpec)
            .addAll(super.getPropertySpecs())
            .build();
    }

    private String getNodesArg(NodeGroup nodeGroup)
    {
        int numNodes = nodeGroup.getNodes().size();
        if (regionSpec.optionalValue(nodeGroup).isPresent())
        {
            numNodes /= DEFAULT_ZONES_PER_REGION;
        }
        return String.format("--num-nodes %s", numNodes);
    }

    private String getRegionOrZoneArg()
    {
        return regionSpec.optionalValue(getNodeGroup())
            .map(region -> String.format("--region %s", region))
            .orElse(String.format("--zone %s", zoneSpec.value(getNodeGroup())));
    }

    @Override
    public String generateClusterName(NodeGroup nodeGroup, Optional<User> user, TestRunIdentifier testRunIdentifier)
    {
        return ClusterNames.generateGCEClusterName(nodeGroup, testRunIdentifier);
    }

    @Override
    public Optional<ResourceRequirement> getResourceRequirements(NodeGroup nodeGroup)
    {
        return explicitClusterName(nodeGroup).map(clusterName -> {
            String provider = name();
            String project = projectSpec.value(nodeGroup);
            String machineType = machineTypeSpec.value(nodeGroup);

            ResourceType resourceType =
                new ResourceType(
                    provider,
                    project,
                    machineType,
                    Optional.of(clusterName));

            return new ResourceRequirement(resourceType, nodeGroup.getNodes().size());
        });
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        // This will throw if the supplied service account email is bad
        getServiceAccount();
        if (gkeMasterVersionSpec.value(properties) != null)
        {
            validateClusterVersion();
        }
        if (regionSpec.optionalValue(properties).isPresent() == zoneSpec.optionalValue(properties).isPresent())
        {
            throw new PropertySpec.ValidationException(String.format("Exactly one of %s or %s must be specified.",
                regionSpec.name(), zoneSpec.name()));
        }
        if (regionSpec.optionalValue(properties).isPresent() &&
            getNodeGroup().getNodes().size() % DEFAULT_ZONES_PER_REGION != 0)
        {
            throw new PropertySpec.ValidationException(String.format(
                "Regional clusters by default replicate nodes over %d zones; this cant be done when %s=%d.",
                DEFAULT_ZONES_PER_REGION, NODE_COUNT_KEY, getNodeGroup().getNodes().size()));
        }
        checkRegionOrZone(regionSpec, properties, 2);
        checkRegionOrZone(zoneSpec, properties, 3);
    }

    private void checkRegionOrZone(PropertySpec<String> spec, PropertyGroup properties, int expectedLength)
    {
        spec.optionalValue(properties).ifPresent(v -> {
            if (v.split("-").length != expectedLength)
            {
                throw new InvalidConfigurationException(String.format(
                    "Invalid gcloud %s: '%s' - Valid regions look like 'us-west2' and valid zones like 'us-west2-a'",
                    spec.shortName(), v));
            }
        });
    }

    public void validateEnsemble(EnsembleValidator validator)
    {
        String requestedClusterName = clusterName(getNodeGroup());
        if (requestedClusterName.length() >= 40)
        {
            validator.addValidationError(
                String.format("GKE cluster name '%s' must be < 40 characters.", requestedClusterName) +
                    " Try setting an explicit cluster name on the provisioner as a workaround (see FAL-1561).");
        }
    }

    private void validateClusterVersion() throws PropertySpec.ValidationException
    {
        NodeGroup nodeGroup = getNodeGroup();
        List<String> validVersions = getValidGKEClusterVersions();
        if (!validVersions.isEmpty() && !validVersions.contains(gkeMasterVersionSpec.value(nodeGroup)))
        {
            throw new PropertySpec.ValidationException(String.format(
                "Invalid cluster version: %s Valid versions: %s", gkeMasterVersionSpec.value(nodeGroup),
                validVersions));
        }
    }

    @VisibleForTesting
    public List<String> getValidGKEClusterVersions()
    {
        NodeGroup nodeGroup = getNodeGroup();
        authenticateServiceAccount();
        FullyBufferedNodeResponse getServerConfig = executeInKubernetesEnv(String.format(
            "gcloud container get-server-config --project %s --region %s --format json", projectSpec.value(nodeGroup),
            regionSpec.value(nodeGroup)))
                .buffered();
        if (!getServerConfig.waitForSuccess())
        {
            nodeGroup.logger().warn(
                "Could not get gke cluster versions. Will attempt the test run without validation.");
            return List.of();
        }
        String validMasterVersionsJson = JsonUtils.getJsonNode(getServerConfig.getStdout(), "/validMasterVersions")
            .toString();
        return JsonUtils.fromJsonList(validMasterVersionsJson);
    }

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        if (authenticateServiceAccount())
        {
            return CheckResourcesResult.AVAILABLE;
        }
        return CheckResourcesResult.FAILED;
    }

    private User.GoogleCloudServiceAccount getServiceAccount()
    {
        return getNodeGroup().getCredentials().getUser()
            .getGoogleCloudServiceAccount(serviceAccountSpec.optionalValue(getNodeGroup()));
    }

    private Path createTemporaryServiceAccountFile()
    {
        Path tempServiceAccountKey =
            getNodeGroup().getLocalScratchSpace().resolve("gcloud-service-account.json");
        if (!tempServiceAccountKey.toFile().exists())
        {
            FileUtils.writeString(tempServiceAccountKey, getServiceAccount().keyFileJson);
        }
        return tempServiceAccountKey;
    }

    private boolean authenticateServiceAccount()
    {
        Path tempServiceAccountKey = createTemporaryServiceAccountFile();

        String authenticateServiceAccountCmd = String.format(
            "gcloud auth activate-service-account --key-file=%s", tempServiceAccountKey.toAbsolutePath());

        NodeResponse authenticate = executeInKubernetesEnv(authenticateServiceAccountCmd);
        return authenticate.waitForSuccess();
    }

    @Override
    protected boolean createKubernetesCluster(NodeGroup nodeGroup)
    {
        clusterCreationInstant = Instant.now();
        String clusterVersionArg = gkeMasterVersionSpec.value(nodeGroup) != null ?
            String.format(" --cluster-version %s", gkeMasterVersionSpec.value(nodeGroup)) :
            "";
        String customLabels = FalloutPropertySpecs.testRunId.optionalValue(nodeGroup)
            .map(testRunId -> String.format("--labels=fallout_testrun_id=%s", testRunId))
            .orElse("");
        String createClusterCmd = String.format(
            "gcloud container clusters create %s --project %s %s %s --machine-type %s %s%s %s",
            clusterName(nodeGroup), projectSpec.value(nodeGroup), getRegionOrZoneArg(),
            customLabels,
            machineTypeSpec.value(nodeGroup), getNodesArg(nodeGroup), clusterVersionArg,
            extraCreateArgsSpec.optionalValue(nodeGroup).orElse(""));
        return executeInKubernetesEnv(createClusterCmd).waitForSuccess();
    }

    @Override
    protected Optional<Path> getProvisionerStorageClassYaml()
    {
        final var storageYaml = "gke-fallout-storage.yaml";
        final var storageYamlPath = getNodeGroup().getLocalScratchSpace().makeScratchSpaceFor(this)
            .resolve(storageYaml);

        FileUtils.writeString(storageYamlPath,
            ResourceUtils.getResourceAsString(getClass(), storageYaml));

        return Optional.of(storageYamlPath);
    }

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        // capture user namespaces created during previous test runs
        userNamespaces.addAll(nodeGroup.findFirstRequiredProvider(KubeControlProvider.class).getNamespaces());
        userNamespaces.removeAll(List.of("kube-node-lease", "kube-public", "kube-system"));
        return true;
    }

    private String getLogId(String namespace, String podName, String containerName)
    {
        return String.join("_", namespace, podName, containerName);
    }

    private Writer getContainerLogWriter(NodeGroup nodeGroup, String namespace, String podName, String containerName)
    {
        Path containerLog = nodeGroup.getLocalArtifactPath().resolve(
            Paths.get("gke-logs", namespace, podName, String.format("%s.log", containerName)));
        try
        {
            // ensure namespace and pod directories exists
            // may have already been created by another log line
            Files.createDirectories(containerLog.getParent());
            return new FileWriter(containerLog.toFile());
        }
        catch (IOException e)
        {
            nodeGroup.logger().error(
                String.format("Could not create FileWriter for log: (%s). Using NullWriter instead.",
                    getLogId(namespace, podName, containerName)),
                e);
            return Writer.nullWriter();
        }
    }

    @Override
    public boolean stopImpl(NodeGroup nodeGroup)
    {
        boolean collectedLogs = !collectGkeLogsSpec.value(nodeGroup) || collectLogsFromGke();
        return super.stopImpl(nodeGroup) && collectedLogs;
    }

    private boolean collectLogsFromGke()
    {
        GoogleCredentials credentials = Exceptions.getUncheckedIO(() -> GoogleCredentials.fromStream(
            Files.newInputStream(createTemporaryServiceAccountFile())));

        LoggingSettings loggingSettings = Exceptions.getUncheckedIO(() -> LoggingSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
            .build());

        try (LoggingClient loggingClient = LoggingClient.create(loggingSettings))
        {
            List<CompletableFuture<Boolean>> futures = userNamespaces.stream()
                .map(namespace -> handleNamespaceLogs(loggingClient, namespace))
                .toList();
            return Utils.waitForAll(futures);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String formatFilterTimestamp(Instant instant)
    {
        // timestamp>="2020-06-17T21:00:00Z"
        String res = DateUtils.formatUTCDate(instant);
        return res.replace(" ", "T") + "Z";
    }

    private CompletableFuture<Boolean> handleNamespaceLogs(LoggingClient loggingClient, String namespace)
    {
        // https://cloud.google.com/logging/docs/view/advanced-queries
        String filter = String.format("resource.labels.cluster_name=\"%s\" AND resource.labels.namespace_name=\"%s\"",
            clusterName(getNodeGroup()), namespace);

        // TODO: improve date source after FAL-1509
        if (clusterCreationInstant != null)
        {
            filter += String.format(" AND timestamp >= \"%s\"", formatFilterTimestamp(clusterCreationInstant));
        }
        getNodeGroup().logger().info("handleNamespaceLogs uses filter: " + filter);

        ListLogEntriesRequest request = ListLogEntriesRequest.newBuilder()
            .addResourceNames(String.format("projects/%s", getServiceAccount().project))
            .setFilter(filter)
            .setOrderBy("timestamp asc")
            .setPageSize(1000)
            .build();

        return CompletableFuture.supplyAsync(() -> {
            Map<String, Writer> containerLogWriters = new HashMap<>();
            try
            {
                // Block until initial page request can be made.
                gcloudLoggingApiRateLimiter.acquire();
                ScopedLogger.getLogger(getNodeGroup().logger())
                    .withScopedInfo("Fetching logs for namespace: {}", namespace).run(() -> {
                        LoggingClient.ListLogEntriesPagedResponse entries = loggingClient.listLogEntries(request);
                        for (LoggingClient.ListLogEntriesPage page : entries.iteratePages())
                        {
                            for (LogEntry entry : page.getValues())
                            {
                                Map<String, String> labels = entry.getResource().getLabelsMap();

                                if (!labels.containsKey("pod_name") || !labels.containsKey("container_name"))
                                {
                                    continue;
                                }

                                String podName = labels.get("pod_name");
                                String containerName = labels.get("container_name");
                                String textPayload = entry.getTextPayload();

                                String jsonPayload;
                                try
                                {
                                    jsonPayload = JsonFormat.printer().print(entry.getJsonPayload());
                                }
                                catch (InvalidProtocolBufferException e)
                                {
                                    jsonPayload = "";
                                }

                                String logLine;

                                if (textPayload.isEmpty() && jsonPayload.isEmpty())
                                {
                                    continue;
                                }
                                else if (!textPayload.isEmpty())
                                {
                                    // let unstructured log take precedence over structured log
                                    logLine = textPayload;
                                }
                                else
                                {
                                    logLine = jsonPayload;
                                }

                                try
                                {
                                    containerLogWriters
                                        .computeIfAbsent(getLogId(namespace, podName, containerName),
                                            ignored -> getContainerLogWriter(getNodeGroup(), namespace, podName,
                                                containerName))
                                        .append(logLine);
                                }
                                catch (IOException e)
                                {
                                    getNodeGroup().logger().error("Error writing log line to log file", e);
                                }
                            }
                            // Block until next page request can be made.
                            gcloudLoggingApiRateLimiter.acquire();
                            getNodeGroup().logger().info(
                                "Fetching next log entry page page for namespace: {}", namespace);
                        }
                    });
            }
            catch (ResourceExhaustedException e)
            {
                getNodeGroup().logger().error("Exceeded quota reading logs, but will not fail the test.", e);
                return true;
            }
            finally
            {
                containerLogWriters.values().forEach(writer -> Exceptions.runUncheckedIO(writer::close));
            }
            return true;
        });
    }

    @Override
    protected void onNamespaceCreation(String namespace)
    {
        userNamespaces.add(namespace);
    }

    private record GCEDisk(String zone, String name) {
        private static String toString(Collection<GCEDisk> gceDisks)
        {
            return gceDisks.stream().map(GCEDisk::toString)
                .collect(Collectors.joining("\n  ", "  ", ""));
        }
    }

    private boolean getGCEDisksFromGCloudJSON(String gcloudJson, Set<GCEDisk> gceDisks)
    {
        try
        {
            for (final var item : JsonUtils.getJsonNode(gcloudJson))
            {
                // The JSON format of gcloud compute disks has the zone as a URL, while the output of
                // kubectl get pv has an unadorned zone.
                final var zonePath = URI.create(item.get("zone").require().asText()).getPath();
                final var zone = zonePath.substring(zonePath.lastIndexOf('/') + 1);
                gceDisks.add(new GCEDisk(zone, item.get("name").require().asText()));
            }
        }
        catch (Throwable ex)
        {
            getNodeGroup().logger().error("Could not read all GCE disks", ex);
            return false;
        }
        return true;
    }

    private boolean deleteLeakedDisks(HashSet<GCEDisk> gceDisksInCluster)
    {
        final var gCloudListDisksJsonCommand = executeInKubernetesEnv(
            "gcloud compute disks list --project %s --format json".formatted(
                projectSpec.value(getNodeGroup())))
                    .buffered();

        final var existingGCEDisks = new HashSet<GCEDisk>();

        final var gCloudListDisksJsonSucceeded =
            gCloudListDisksJsonCommand.doWait().forSuccess() &&
                getGCEDisksFromGCloudJSON(gCloudListDisksJsonCommand.getStdout(), existingGCEDisks);

        if (!gCloudListDisksJsonSucceeded)
        {
            getNodeGroup().logger().error("Could not get existing GCE disks; " +
                "attempting to delete all disks in cluster even if they no longer exist");
        }
        else
        {
            getNodeGroup().logger().info("Found the following existing GCE disks:\n{}",
                GCEDisk.toString(existingGCEDisks));

            gceDisksInCluster.retainAll(existingGCEDisks);

            if (!gceDisksInCluster.isEmpty())
            {
                getNodeGroup().logger().warn("The following GCE disks were leaked by the cluster; " +
                    "attempting to delete them now:\n{}", GCEDisk.toString(gceDisksInCluster));
            }
        }

        final var deleteDiskCommands = gceDisksInCluster.stream()
            .map(disk -> executeInKubernetesEnv(
                "gcloud compute disks delete --project %s --quiet --zone %s %s".formatted(
                    projectSpec.value(getNodeGroup()),
                    disk.zone(),
                    disk.name())))
            .toList();

        return Utils.waitForSuccess(getNodeGroup().logger(), deleteDiskCommands) && gCloudListDisksJsonSucceeded;
    }

    private boolean getGCEDisksFromPVJson(String pvJson, Set<GCEDisk> gceDisks)
    {
        try
        {
            final var rootNode = JsonUtils.getJsonNode(pvJson);

            for (final var item : rootNode.at("/items").require())
            {
                // Ignore PVs that are not dynamic (https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/),
                // operating under the assumption that static PVs will be backed by disks containing
                // data that should not be deleted between testruns (for example, test data).
                final var createdBy = item.at("/metadata/annotations").get("kubernetes.io/createdby");
                if (createdBy != null && createdBy.asText().equals("gce-pd-dynamic-provisioner"))
                {
                    gceDisks.add(new GCEDisk(
                        item.at("/metadata/labels").get("topology.kubernetes.io/zone").require().asText(),
                        item.at("/spec/gcePersistentDisk/pdName").require().asText()));
                }
            }
        }
        catch (Throwable ex)
        {
            getNodeGroup().logger().error("Could not read all PVs from cluster", ex);
            return false;
        }

        return true;
    }

    @Override
    protected boolean destroyKubernetesCluster(NodeGroup nodeGroup)
    {
        final var getPVJsonCommand =
            executeInKubernetesEnv("kubectl get persistentvolumes -o json").buffered();

        final var gceDisksInCluster = new HashSet<GCEDisk>();

        final var getPVJsonSucceeded =
            getPVJsonCommand.doWait().forSuccess() &&
                getGCEDisksFromPVJson(getPVJsonCommand.getStdout(), gceDisksInCluster);

        if (!getPVJsonSucceeded)
        {
            nodeGroup.logger().error("Could not get all PVs from cluster; " +
                "YOU MUST MANUALLY CHECK FOR LEAKED GCE DISKS AFTER THIS CLUSTER IS DESTROYED");
        }
        else
        {
            nodeGroup.logger().info("Found the following GCE disks backing cluster PVs:\n{}",
                GCEDisk.toString(gceDisksInCluster));
        }

        final var clusterDestroyed =
            executeInKubernetesEnv(
                "gcloud container clusters delete %s --project %s %s".formatted(
                    clusterName(nodeGroup),
                    projectSpec.value(nodeGroup),
                    getRegionOrZoneArg()))
                        .waitForSuccess();

        return getPVJsonSucceeded &&
            clusterDestroyed &&
            deleteLeakedDisks(gceDisksInCluster);
    }

    @Override
    protected NodeGroup.State checkExistingKubernetesClusterState(NodeGroup nodeGroup)
    {
        if (!authenticateServiceAccount())
        {
            return NodeGroup.State.FAILED;
        }

        String clusterExistsCmd = String.format("gcloud container clusters get-credentials %s --project %s %s",
            clusterName(nodeGroup), projectSpec.value(nodeGroup), getRegionOrZoneArg());
        NodeResponse clusterExists = executeInKubernetesEnv(clusterExistsCmd);
        if (!clusterExists.waitForOptionalSuccess())
        {
            return NodeGroup.State.DESTROYED;
        }
        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    @Override
    public Map<String, String> getKubernetesEnv()
    {
        Map<String, String> env = new HashMap<>(super.getKubernetesEnv());
        if (cloudSdkConfigPath == null)
        {
            cloudSdkConfigPath = getNodeGroup().getLocalScratchSpace()
                .makeScratchSpaceFor("gcloud-sdk-config").getPath();
        }
        env.put(CLOUD_SDK_CONFIG_ENV_VAR, cloudSdkConfigPath.toString());
        env.put("CLOUDSDK_CORE_DISABLE_PROMPTS", "1");
        return env;
    }
}
