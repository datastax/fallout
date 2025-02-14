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

import java.util.Optional;

import com.datastax.fallout.ops.Node;

// Represents a Prometheus server that can scrape as well as have metrics pushed to it
public class PrometheusServerPushProvider extends PrometheusServerProvider
{
    public static final String promPushKeySubdir = "prompush";

    public PrometheusServerPushProvider(Node node, String host, Integer port, Optional<String> apiKey)
    {
        super(node, host, port, apiKey);
    }

    @Override
    public String name()
    {
        return "PrometheusServerPush";
    }

    public String getImportEndpoint()
    {
        return String.format("%s:%d/api/v1/import", host, port);
    }
}
