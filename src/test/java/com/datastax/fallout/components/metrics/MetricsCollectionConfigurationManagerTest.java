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

import javax.ws.rs.NotFoundException;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.util.ResourceUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.assertj.Assertions.assertThatThrownBy;
import static com.datastax.fallout.components.metrics.MetricsCollectionConfigurationManager.PROMETHEUS_API_DEFAULT_QUERY_RANGE_STEP;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.mockito.Mockito.*;

class MetricsCollectionConfigurationManagerTest
{
    private static WireMockServer wireMockServer;
    private static final int PORT = 9090;

    private static final String metricName = "metric_a";
    private static final Instant instant = Instant.now();
    private static final Supplier<Instant> instantSupplier = () -> instant;
    private static final String nonExistingMetricName = "non-existing";
    private static String jsonFileContent;

    @BeforeAll
    public static void setup() throws IOException
    {
        jsonFileContent = ResourceUtils.readResourceAsString(MetricsCollectionConfigurationManagerTest.class,
            "metric_a.json");

        wireMockServer = new WireMockServer(options().port(PORT));
        wireMockServer.start();
        wireMockServer.stubFor(
            get(urlEqualTo(
                String.format("/api/v1/query_range?query=%s&start=%s&end=%s&step=%s", metricName,
                    instant.getEpochSecond(), instant.getEpochSecond(), PROMETHEUS_API_DEFAULT_QUERY_RANGE_STEP)))
                        .willReturn(
                            aResponse()
                                .withStatus(200)
                                .withBody(jsonFileContent)
                                .withHeader("Content-Type", "application/json")));
        wireMockServer.stubFor(
            get(urlEqualTo(
                String.format("/api/v1/query_range?query=%s&start=%s&end=%s&step=%s", nonExistingMetricName,
                    instant.getEpochSecond(), instant.getEpochSecond(), PROMETHEUS_API_DEFAULT_QUERY_RANGE_STEP)))
                        .willReturn(
                            aResponse()
                                .withStatus(404))
        );
    }

    @AfterAll
    public static void cleanup()
    {
        if (wireMockServer != null)
        {
            wireMockServer.stop();
        }
    }

    @Test
    public void shouldGetMetricFromHttpEndpoint() throws IOException
    {
        MetricsCollectionConfigurationManager metricsCollectionConfigurationManager =
            new MetricsCollectionConfigurationManager(instantSupplier);
        metricsCollectionConfigurationManager.configureImpl(mock(NodeGroup.class));

        String jsonResponse =
            metricsCollectionConfigurationManager.executeGetRequestForJsonContent(metricName, prometheusUrl());

        assertThat(jsonResponse).isEqualTo(jsonFileContent);
    }

    private String prometheusUrl()
    {
        return "http://localhost:" + PORT;
    }

    @Test
    public void shouldThrowWhenGetNonExistingMetricFromHttpEndpoint() throws IOException
    {
        MetricsCollectionConfigurationManager metricsCollectionConfigurationManager =
            new MetricsCollectionConfigurationManager(instantSupplier);
        metricsCollectionConfigurationManager.configureImpl(mock(NodeGroup.class));

        assertThatThrownBy(
            () -> metricsCollectionConfigurationManager.executeGetRequestForJsonContent(nonExistingMetricName,
                prometheusUrl()))
                    .isInstanceOf(NotFoundException.class)
                    .hasMessageContaining("404");
    }

    @Test
    public void shouldCollectMetricsIntoArtifactsDirectoryFromPrometheusAPI()
    {
        Instant start = Instant.now();
        AtomicReference<Instant> nowReference = new AtomicReference<>(start);
        Supplier<Instant> instantSupplier = nowReference::get;
        MetricsCollectionConfigurationManager metricsCollectionConfigurationManager =
            new MetricsCollectionConfigurationManager(instantSupplier);
        metricsCollectionConfigurationManager.configureImpl(mock(NodeGroup.class));

        String metricName = "metric_a";
        Instant end = start.plus(Duration.ofSeconds(100));
        nowReference.set(end);
        String curlCommand = metricsCollectionConfigurationManager.constructMetricsUrl(metricName, prometheusUrl());

        assertThat(curlCommand).isEqualTo(
            String.format(
                "http://localhost:9090/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
                metricName, start.getEpochSecond(), end.getEpochSecond(), PROMETHEUS_API_DEFAULT_QUERY_RANGE_STEP)
        );
    }
}
