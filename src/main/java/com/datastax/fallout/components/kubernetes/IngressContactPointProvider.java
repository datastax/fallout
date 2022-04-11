/*
 * Copyright 2022 DataStax, Inc.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.datastax.fallout.components.common.provider.ServiceContactPointProvider;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.util.JsonUtils;

public class IngressContactPointProvider extends ServiceContactPointProvider
{
    private final String ingressName;
    private final Optional<String> namespace;

    public IngressContactPointProvider(Node node, String serviceName, String ingressName)
    {
        this(node, serviceName, ingressName, Optional.empty());
    }

    /**
     * Users cannot naturally specify an optional type in YAML format. This override allows users to declare a
     * non-default namespace for the Ingress object.
     */
    public IngressContactPointProvider(Node node, String serviceName, String ingressName, String namespace)
    {
        this(node, serviceName, ingressName, Optional.of(namespace));
    }

    public IngressContactPointProvider(Node node, String serviceName, String ingressName, Optional<String> namespace)
    {
        super(node, serviceName);
        this.ingressName = ingressName;
        this.namespace = namespace;
    }

    @Override
    public String getContactPoint()
    {
        return node().getProvider(KubeControlProvider.class).inNamespace(namespace, namespacedKubeCtl -> {
            var getIngressRes =
                namespacedKubeCtl.execute(String.format("get ingress %s --output json", ingressName)).buffered();
            if (!getIngressRes.waitForSuccess())
            {
                throw new RuntimeException("Error getting ingress definition");
            }
            return JsonUtils.getJsonNode(getIngressRes.getStdout(), "/status/loadBalancer/ingress").get(0).get("ip")
                .asText();
        });
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        var required = new HashSet<>(super.getRequiredProviders());
        required.add(KubeControlProvider.class);
        return required;
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        var infoMap = new HashMap<>(super.toInfoMap());
        infoMap.put("external_ip", getContactPoint());
        return infoMap;
    }

    @Override
    public boolean isNodeGroupProvider()
    {
        return true;
    }
}
