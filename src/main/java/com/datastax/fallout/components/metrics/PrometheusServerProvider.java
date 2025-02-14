/*
 * Copyright 2025 DataStax, Inc.
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

import java.util.Map;
import java.util.Optional;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;

// Represents a Prometheus server that scrapes metrics from source(s)
public class PrometheusServerProvider extends Provider
{
    protected final String host;
    protected final Integer port;
    protected final Optional<String> apiKey;

    public PrometheusServerProvider(Node node, String host, Integer port)
    {
        this(node, host, port, Optional.empty());
    }

    public PrometheusServerProvider(Node node, String host, Integer port, Optional<String> apiKey)
    {
        super(node);
        this.host = host;
        this.port = port;
        this.apiKey = apiKey;
    }

    @Override
    public String name()
    {
        return "PrometheusServerProvider";
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        return Map.of(
            "host", host,
            "port", String.valueOf(port)
        );
    }

    public String getHost()
    {
        return host;
    }

    public Integer getPort()
    {
        return port;
    }

    public Optional<String> getApiKey()
    {
        return apiKey;
    }

    public String getMetricsEndpoint()
    {
        return String.format("%s:%d/metrics", host, port);
    }

    public String getQueryEndpoint()
    {
        return String.format("%s:%d/api/v1/query", host, port);
    }

    public String getQueryRangeEndpoint()
    {
        return String.format("%s:%d/api/v1/query_range", host, port);
    }

}
