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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.ops.*;
import com.datastax.fallout.ops.provisioner.NoRemoteAccessProvisioner;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.util.HttpUtils;

@AutoService(Provisioner.class)
public class ExistingPromPushProvisioner extends NoRemoteAccessProvisioner
{
    private static final String PREFIX = "fallout.configuration.management.existing_prompush.";
    private static final String NAME = "existing_prompush";
    private static final String DESCRIPTION = "For exporting metrics to an existing Prometheus server";

    static final PropertySpec<String> hostUriSpec = PropertySpecBuilder
        .createStr(PREFIX)
        .name("host")
        .description("Host URI for the existing Prometheus server")
        .required()
        .build();

    static final PropertySpec<Integer> portSpec = PropertySpecBuilder
        .createInt(PREFIX)
        .name("port")
        .description("The port for the existing Prometheus server")
        .required()
        .build();

    // TODO: Add other options for secure connection
    static final PropertySpec<String> apiKeySpec = PropertySpecBuilder
        .createStr(PREFIX)
        .name("api_key")
        .description("The API key for using the existing Prometheus server")
        .required(false)
        .build();

    public ExistingPromPushProvisioner()
    {
        super(NAME);
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(hostUriSpec)
            .add(portSpec)
            .add(apiKeySpec)
            .addAll(super.getPropertySpecs())
            .build();
    }

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
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        NodeGroup nodeGroup = getNodeGroup();
        if (nodeGroup.getNodes().size() != 1)
        {
            throw new PropertySpec.ValidationException("ExistingPromPush nodegroup must have 1 node!");
        }
    }

    @Override
    public void doSummarizeInfo(InfoConsumer infoConsumer)
    {
        getNodeGroup().findFirstProvider(PrometheusServerPushProvider.class)
            .ifPresent(p -> {
                infoConsumer.accept("host", p.getHost());
                infoConsumer.accept("port", p.getPort());
            });
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup properties)
    {
        return Set.of(PrometheusServerPushProvider.class);
    }

    @Override
    protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.AVAILABLE;
    }

    @Override
    protected CheckResourcesResult createImpl(NodeGroup nodeGroup)
    {
        return CheckResourcesResult.FAILED;
    }

    @Override
    protected boolean prepareImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        if (!createProviders(nodeGroup))
            return NodeGroup.State.FAILED;
        PrometheusServerPushProvider provider = nodeGroup.findFirstRequiredProvider(PrometheusServerPushProvider.class);
        // fail fast if issues connecting to Pometheus
        String uri = provider.getMetricsEndpoint();
        try
        {
            HttpUtils.httpGetString(uri, provider.getAuthToken());
            nodeGroup.logger().info("Successfully verified existing Prometheus server");
            return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
        }
        catch (IOException e)
        {
            nodeGroup.logger().error("Failed to connect to existing Prometheus server");
            return NodeGroup.State.FAILED;
        }
    }

    private boolean createProviders(NodeGroup nodeGroup)
    {
        String hostUri = hostUriSpec.value(nodeGroup.getProperties());
        Integer promPushPort = portSpec.value(nodeGroup.getProperties());
        Optional<String> apiKey = apiKeySpec.optionalValue(nodeGroup.getProperties());
        new PrometheusServerPushProvider(
            getNodeGroup().getNodes().get(0),
            hostUri,
            promPushPort,
            apiKey
        );
        return true;
    }

    @Override
    protected boolean startImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected boolean stopImpl(NodeGroup nodeGroup)
    {
        return true;
    }

    @Override
    protected boolean destroyImpl(NodeGroup nodeGroup)
    {
        nodeGroup.getNodes().get(0).maybeUnregister(PrometheusServerPushProvider.class);
        return true;
    }

}
