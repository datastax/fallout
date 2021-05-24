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
package com.datastax.fallout.components.common.provider;

import java.util.HashMap;
import java.util.Map;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;

/**
 * Provide basic info about a node
 */
public class NodeInfoProvider extends Provider
{
    private final String publicNetworkAddress;
    private final String privateNetworkAddress;
    private final String userName;

    public NodeInfoProvider(Node node, String publicNetworkAddress, String privateNetworkAddress, String userName)
    {
        super(node);
        this.publicNetworkAddress = publicNetworkAddress;
        this.privateNetworkAddress = privateNetworkAddress;
        this.userName = userName;
    }

    @Override
    public String name()
    {
        return "node-info";
    }

    public String getPublicNetworkAddress()
    {
        return publicNetworkAddress;
    }

    public String getPrivateNetworkAddress()
    {
        return privateNetworkAddress;
    }

    public String getUserName()
    {
        return userName;
    }

    @Override
    public String toString()
    {
        return String
            .format("node: %d, public-address: %s, private-address: %s, username: %s", node().getNodeGroupOrdinal(),
                publicNetworkAddress, privateNetworkAddress, userName);
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        return new HashMap<String, String>() {
            {
                put("node", String.valueOf(node().getNodeGroupOrdinal()));
                put("publicNetworkAddress", publicNetworkAddress);
                put("privateNetworkAddress", privateNetworkAddress);
                put("userName", userName);
            }
        };
    }
}
