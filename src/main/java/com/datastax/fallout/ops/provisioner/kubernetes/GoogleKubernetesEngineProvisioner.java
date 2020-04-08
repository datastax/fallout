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
package com.datastax.fallout.ops.provisioner.kubernetes;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.ops.ClusterNames;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;

import static com.datastax.fallout.harness.ActiveTestRunBuilder.NODE_COUNT_KEY;

@AutoService(Provisioner.class)
public class GoogleKubernetesEngineProvisioner extends AbstractKubernetesProvisioner
{
    private static final String prefix = "fallout.provisioner.kubernetes.google.";
    private static final String CLOUD_SDK_CONFIG_ENV_VAR = "CLOUDSDK_CONFIG";
    private static final int DEFAULT_ZONES_PER_REGION = 3;
    private Path cloudSdkConfigPath;

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
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(projectSpec, regionSpec, zoneSpec, machineTypeSpec, gkeMasterVersionSpec, extraCreateArgsSpec,
                serviceAccountSpec)
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
    public String generateClusterName(NodeGroup nodeGroup, String testRunName,
        TestRun testRun, User user)
    {
        return ClusterNames.generateGCEClusterName(nodeGroup, testRunName, testRun, user);
    }

    @Override
    public Optional<ResourceRequirement> getResourceRequirements(NodeGroup nodeGroup)
    {
        return explicitClusterName(nodeGroup).map(clusterName -> {
            String provider = name();
            String project = projectSpec.value(nodeGroup);
            String machineType = machineTypeSpec.value(nodeGroup);

            ResourceRequirement.ResourceType resourceType =
                new ResourceRequirement.ResourceType(provider, project, machineType, Optional.of(clusterName));

            ResourceRequirement requirement = new ResourceRequirement(resourceType, nodeGroup.getNodes().size());
            requirement.setReservationLockResourceType(resourceType);

            return requirement;
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
        authenticateServiceAccount(nodeGroup);
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
        String validMasterVersionsJson = Utils.getJsonNode(getServerConfig.getStdout(), "/validMasterVersions")
            .toString();
        return Utils.fromJsonList(validMasterVersionsJson);
    }

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        if (authenticateServiceAccount(nodeGroup))
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

    private boolean authenticateServiceAccount(NodeGroup nodeGroup)
    {
        Path tempServiceAccountKey = nodeGroup.getLocalScratchSpace().createFile("gcloud-service-account-", ".json");
        Utils.writeStringToFile(tempServiceAccountKey.toFile(), getServiceAccount().keyFileJson);

        String authenticateServiceAccountCmd = String.format(
            "gcloud auth activate-service-account --key-file=%s", tempServiceAccountKey.toAbsolutePath());

        NodeResponse authenticate = executeInKubernetesEnv(authenticateServiceAccountCmd);
        return authenticate.waitForSuccess();
    }

    @Override
    protected boolean createKubernetesCluster(NodeGroup nodeGroup)
    {
        String clusterVersionArg = gkeMasterVersionSpec.value(nodeGroup) != null ?
            String.format(" --cluster-version %s", gkeMasterVersionSpec.value(nodeGroup)) :
            "";
        String createClusterCmd = String.format(
            "gcloud container clusters create %s --project %s %s --machine-type %s %s%s %s",
            clusterName(nodeGroup), projectSpec.value(nodeGroup), getRegionOrZoneArg(),
            machineTypeSpec.value(nodeGroup), getNodesArg(nodeGroup), clusterVersionArg,
            extraCreateArgsSpec.value(nodeGroup));
        return executeInKubernetesEnv(createClusterCmd).waitForSuccess();
    }

    @Override
    protected boolean destroyKubernetesCluster(NodeGroup nodeGroup)
    {
        String destroyClusterCmd = String.format(
            "gcloud container clusters delete %s --quiet --project %s %s", clusterName(nodeGroup),
            projectSpec.value(nodeGroup), getRegionOrZoneArg());
        NodeResponse destroy = executeInKubernetesEnv(destroyClusterCmd);
        return destroy.waitForSuccess();
    }

    @Override
    protected NodeGroup.State checkExistingKubernetesClusterState(NodeGroup nodeGroup)
    {
        if (!authenticateServiceAccount(nodeGroup))
        {
            return NodeGroup.State.FAILED;
        }

        String clusterExistsCmd = String.format("gcloud container clusters get-credentials %s --project %s %s",
            clusterName(nodeGroup), projectSpec.value(nodeGroup), getRegionOrZoneArg());
        NodeResponse clusterExists = executeInKubernetesEnv(clusterExistsCmd);
        if (!clusterExists.doWait().withNonZeroIsNoError().forSuccess())
        {
            return NodeGroup.State.DESTROYED;
        }
        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    @Override
    public Map<String, String> getKubernetesEnv()
    {
        Map<String, String> env = super.getKubernetesEnv();
        if (cloudSdkConfigPath == null)
        {
            cloudSdkConfigPath = getNodeGroup().getLocalScratchSpace().createDirectory("gcloud-sdk-config-");
        }
        env.put(CLOUD_SDK_CONFIG_ENV_VAR, cloudSdkConfigPath.toString());
        return env;
    }
}
