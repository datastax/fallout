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

public class PromPushProvider extends Provider
{
    public static final String prompushKeySubdir = "prompush";
    private final String hostUri;
    private final Integer promPushPort;
    private final String apiKey;

    public PromPushProvider(Node node, String hostUri, Integer promPushPort, String apiKey)
    {
        super(node);
        this.hostUri = hostUri;
        this.promPushPort = promPushPort;
        this.apiKey = apiKey;
    }

    @Override
    public String name()
    {
        return "prompush";
    }

    public String getHostUri()
    {
        return hostUri;
    }

    public Integer getPort()
    {
        return promPushPort;
    }

    public String getApiKey()
    {
        return apiKey;
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        return Map.of(
            "host_uri", hostUri,
            "prompush_port", String.valueOf(promPushPort)
        );
    }

    @Override
    public boolean isNodeGroupProvider()
    {
        return true;
    }
}
