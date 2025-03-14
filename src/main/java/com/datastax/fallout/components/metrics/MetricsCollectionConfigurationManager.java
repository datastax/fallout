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
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import com.google.auto.service.AutoService;

import com.datastax.fallout.cassandra.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastax.fallout.components.kubernetes.KubeControlProvider;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.service.FalloutClientBuilder;
import com.datastax.fallout.util.FileUtils;

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
        // don't set in configureImpl, to avoid NPE (in case nodegroup already has state STARTED_SERVICES_CONFIGURED)
        clusterCreationInstant = instantSupplier.get();
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
            "For Prometheus deployed in a kubernetes environment, the kubernetes namespace that will be used for find a prometheus service and do the port-forwarding.")
        .required(false)
        .build();

    static final PropertySpec<String> prometheusService = PropertySpecBuilder
        .createStr(PREFIX)
        .name("prometheus_service")
        .description(
            "For Prometheus deployed in a kubernetes environment, name of the prometheus service to collect metrics from.")
        .required(false)
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
        return Set.of(PrometheusServerProvider.class);
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
        String prometheusUrl = null;

        PrometheusServerProvider prometheusServer = node.getProvider(PrometheusServerProvider.class);
        Optional<KubeControlProvider> kubeCtlProvider = node.maybeGetProvider(KubeControlProvider.class);
        Optional<String> promServiceVal = prometheusService.optionalValue(getNodeGroup());
        Optional<String> namespaceVal = namespace.optionalValue(getNodeGroup());

        boolean prometheusRunsInKubernetes = kubeCtlProvider.isPresent();
        boolean falloutRunsInKubernetes = prometheusRunsInKubernetes && !kubeCtlProvider.get().kubeConfigExists();

        if (!prometheusRunsInKubernetes)
        {
            node.getNodeGroup().logger().info("Detected Prometheus is deployed outside Kubernetes.");
            prometheusUrl = String.format("%s:%d", prometheusServer.getHost(), prometheusServer.getPort());
        }
        else
        {
            node.getNodeGroup().logger().info("Detected Prometheus is deployed inside Kubernetes.");
            if (falloutRunsInKubernetes)
            {
                node.getNodeGroup().logger().info("Detected Fallout is deployed inside Kubernetes.");
                prometheusUrl = constructPrometheusServiceBaseUrl(promServiceVal, namespaceVal);
            }
            else
            {
                node.getNodeGroup().logger().info("Detected Fallout is deployed outside Kubernetes.");
            }
        }

        if (prometheusUrl != null)
        {
            saveAllMetricsToArtifacts(node, prometheusUrl, prometheusServer.getAuthToken());
            return;
        }
        if (prometheusRunsInKubernetes)
        {
            handlePrometheusInKubernetes(node, kubeCtlProvider, promServiceVal, namespaceVal);
        }
        else
        {
            node.getNodeGroup().logger().error("Could not resolve Prometheus URL");
        }
    }

    private String constructPrometheusServiceBaseUrl(Optional<String> promServiceVal, Optional<String> namespaceVal)
    {
        if (promServiceVal.isPresent() && namespaceVal.isPresent())
        {
            return String.format(
                "http://%s.%s.svc.cluster.local:%s",
                promServiceVal.get(),
                namespaceVal.get(),
                prometheusServicePort.value(getNodeGroup())
            );
        }
        return null;
    }

    private void handlePrometheusInKubernetes(Node node, Optional<KubeControlProvider> kubeCtlProvider,
        Optional<String> promServiceVal, Optional<String> namespaceVal)
    {
        if (promServiceVal.isEmpty())
        {
            node.getNodeGroup().logger().error("prometheus_service needs to be provided.");
            return;
        }
        if (namespaceVal.isEmpty())
        {
            node.getNodeGroup().logger().error("namespace needs to be provided.");
            return;
        }
        kubeCtlProvider.get().inNamespace(
            namespaceVal,
            namespacedKubeCtl -> namespacedKubeCtl.withPortForwarding(
                promServiceVal.get(),
                prometheusServicePort.value(node),
                localPort -> {
                    saveAllMetricsToArtifacts(
                        node,
                        String.format("http://localhost:%d", localPort)
                    );
                }
            )
        );
    }

    private void saveAllMetricsToArtifacts(Node node, String prometheusUrl)
    {
        saveAllMetricsToArtifacts(node, prometheusUrl, Optional.empty());
    }

    private void saveAllMetricsToArtifacts(Node node, String prometheusUrl, Optional<String> authToken)
    {
        for (String metricToCollectName : metricsToCollect.value(node))
        {
            try
            {
                String metricsJsonContent =
                    executeGetRequestForJsonContent(metricToCollectName, prometheusUrl, authToken);
                if (metricsJsonContent != null)
                {
                    saveMetricsToArtifacts(metricsJsonContent, metricToCollectName);
                }
                else
                {
                    node.getNodeGroup().logger().error("Skipping metric '{}' due to failed retrieval.",
                        metricToCollectName);
                }
            }
            catch (Exception e)
            {
                node.getNodeGroup().logger().error("Failed to collect and save metric '{}': {}", metricToCollectName,
                    e.getMessage(), e);
            }
        }
    }

    @VisibleForTesting
    String executeGetRequestForJsonContent(String metricToCollectName, String serviceUrl)
    {
        return executeGetRequestForJsonContent(metricToCollectName, serviceUrl, Optional.empty());
    }

    @VisibleForTesting
    String executeGetRequestForJsonContent(String metricToCollectName, String serviceUrl, Optional<String> authToken)
    {
        String getMetricsUrl = constructMetricsUrl(metricToCollectName, serviceUrl);
        logger().info("Executing HTTP GET for: {}", getMetricsUrl);
        if (authToken.isEmpty())
            return httpClient.target(getMetricsUrl).request().get(String.class);
        else
            return httpClient.target(getMetricsUrl).request().header("Authorization", "Bearer " + authToken.get())
                .get(String.class);
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

    @Override
    public void close()
    {
        super.close();
        httpClient.close();
    }
}
