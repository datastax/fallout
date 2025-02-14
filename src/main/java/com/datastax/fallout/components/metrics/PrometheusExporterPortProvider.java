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

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;

public class PrometheusExporterPortProvider extends Provider
{
    final int port;

    public PrometheusExporterPortProvider(Node node, int port)
    {
        super(node);
        this.port = port;
    }

    @Override
    public String name()
    {
        return "PrometheusExporterPortProvider";
    }

    public int getPort()
    {
        return port;
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        return Map.of("port", "" + port);
    }
}
