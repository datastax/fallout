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
package com.datastax.fallout.ops;

import com.datastax.fallout.exceptions.InvalidConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Add support for expanding the `provider` property, e.g. <<provider:source:0:node-info:publicNetworkAddress>>
 */
public class ProviderPropertyRefHandler extends PropertyRefExpander.Handler
{
    private static final Logger logger = LoggerFactory.getLogger(ProviderPropertyRefHandler.class);

    private final Ensemble ensemble;

    public ProviderPropertyRefHandler(Ensemble ensemble)
    {
        super("provider");
        this.ensemble = ensemble;
    }

    @Override
    public String expandKey(String providerKey)
    {
        var keyElements = providerKey.split(":");
        if (keyElements.length != 4)
        {
            throw new InvalidConfigurationException(
                String.format(
                    "%s is not a valid provider property. Supported format is: provider:<provider name>:<node group>:<ordinal>:<info map key>",
                    providerKey)
            );
        }
        var nodeGroup = keyElements[0];
        var nodeOrdinal = Integer.parseInt(keyElements[1]);
        var providerName = keyElements[2];
        var requestedInfo = keyElements[3];

        var provider = ensemble
            .getNodeGroupByAlias(nodeGroup)
            .getNodes()
            .get(nodeOrdinal)
            .getProviderByName(providerName);

        if (provider == null)
        {
            logger.debug("No {} provider available yet.", providerName);
            return "";
        }

        var info = provider.toInfoMap().get(requestedInfo);
        if (info == null)
        {
            throw new InvalidConfigurationException(
                String.format("%s is not available in node group %s:%s in provider %s", requestedInfo, nodeGroup,
                    nodeOrdinal, providerName));
        }
        return info;
    }
}
