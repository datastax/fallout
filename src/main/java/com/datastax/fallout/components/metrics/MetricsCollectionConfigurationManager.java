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
package com.datastax.fallout.components.metrics;

import javax.ws.rs.client.Client;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.google.auto.service.AutoService;

import com.datastax.fallout.cassandra.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastax.fallout.components.kubernetes.KubeControlProvider;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.service.FalloutClientBuilder;

@AutoService(ConfigurationManager.class)
public class MetricsCollectionConfigurationManager extends ConfigurationManager
{
    private static final String PREFIX = "fallout.configuration.management.metrics_collection.";
    private static final String NAME = "metrics_collection";
    private static final String DESCRIPTION = "Collects and saves metrics into FALLOUT_ARTIFACT_DIR directory";
    private final Supplier<Instant> instantSupplier;
    private final Client httpClient;
    private Instant clusterCreationInstant;
    static final int PROMETHEUS_API_DEFAULT_QUERY_RANGE_STEP = 15;

    public MetricsCollectionConfigurationManager(Supplier<Instant> instantSupplier)
    {
        this.instantSupplier = instantSupplier;
        httpClient = FalloutClientBuilder.forComponent(MetricsCollectionConfigurationManager.class).build();
    }

    public MetricsCollectionConfigurationManager()
    {
        this(Instant::now);
    }

    static final PropertySpec<List<String>> metricsToCollect = PropertySpecBuilder
        .createStrList(PREFIX)
        .name("metrics_to_collect")
        .description("The list of metrics that will be collected and saved into the artifacts directory.")
        .required()
        .build();

    static final PropertySpec<String> namespace = PropertySpecBuilder
        .createStr(PREFIX)
        .name("namespace")
        .description(
            "The kubernetes namespace that will be used for find a prometheus service and do the port-forwarding.")
        .required()
        .build();

    static final PropertySpec<String> prometheusService = PropertySpecBuilder
        .createStr(PREFIX)
        .name("prometheus_service")
        .description("Name of the prometheus service to collect metrics from.")
        .required()
        .build();

    static final PropertySpec<Integer> prometheusServicePort = PropertySpecBuilder
        .createInt(PREFIX)
        .name("prometheus_service_port")
        .description("Port where the prometheus service can be reached.")
        .defaultOf(9090)
        .build();

    @Override
    public String prefix()
    {
        return PREFIX;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public String description()
    {
        return DESCRIPTION;
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(metricsToCollect, namespace, prometheusService, prometheusServicePort);
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup properties)
    {
        return Set.of(KubeControlProvider.class);
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        clusterCreationInstant = instantSupplier.get();
        return true;
    }

    @Override
    public boolean prepareArtifactsImpl(Node node)
    {
        // metrics will be collected only on the node-0
        if (node.getNodeGroupOrdinal() > 0)
        {
            return true;
        }

        try
        {
            // create metrics dir
            createMetricsArtifactDirectory();

            // collect metrics into artifacts dir
            collectMetricsFromPrometheus(node);
            return true;
        }
        catch (Exception ex)
        {
            logger().error("Problem in prepareArtifactsImpl: ", ex);
            return false;
        }
    }

    private void collectMetricsFromPrometheus(Node node)
    {
        for (String metricToCollectName : metricsToCollect.value(node))
        {
            String serviceUrl = String.format("http://%s.%s.svc.cluster.local:%s",
                prometheusService.value(node),
                namespace.value(node),
                prometheusServicePort.value(node));
            String metricsJsonContent = executeGetRequestForJsonContent(metricToCollectName, serviceUrl);
            saveMetricsToArtifacts(metricsJsonContent, metricToCollectName);
        }
    }

    @VisibleForTesting
    String executeGetRequestForJsonContent(String metricToCollectName, String serviceUrl)
    {
        String getMetricsUrl = constructMetricsUrl(metricToCollectName, serviceUrl);
        logger().info("Executing HTTP GET for: {}", getMetricsUrl);
        return httpClient.target(getMetricsUrl).request().get(String.class);
    }

    @VisibleForTesting
    String constructMetricsUrl(String metricToCollectName, String serviceUrl)
    {
        long start = clusterCreationInstant.getEpochSecond();
        long end = instantSupplier.get().getEpochSecond();

        return String.format("%s/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
            serviceUrl,
            metricToCollectName, start, end,
            PROMETHEUS_API_DEFAULT_QUERY_RANGE_STEP);
    }

    private void saveMetricsToArtifacts(String metricsJsonContent, String metricName)
    {
        Path jsonFilePath = StableMetricsThresholdArtifactChecker
            .getMetricsArtifactsPath(getNodeGroup()).resolve(String.format("%s.json", metricName));
        FileUtils.writeString(jsonFilePath, metricsJsonContent);
    }

    @VisibleForTesting
    void createMetricsArtifactDirectory()
    {
        Path metricsArtifactsPath = StableMetricsThresholdArtifactChecker.getMetricsArtifactsPath(getNodeGroup());
        FileUtils.createDirs(metricsArtifactsPath);
    }
}
