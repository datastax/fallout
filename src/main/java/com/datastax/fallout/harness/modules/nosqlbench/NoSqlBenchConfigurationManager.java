/*
 * Copyright 2020 DataStax, Inc.
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
package com.datastax.fallout.harness.modules.nosqlbench;

import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.configmanagement.kubernetes.KubernetesDeploymentConfigurationManager;
import com.datastax.fallout.ops.providers.DataStaxCassOperatorProvider;

@AutoService(ConfigurationManager.class)
public class NoSqlBenchConfigurationManager extends KubernetesDeploymentConfigurationManager.WithPersistentContainer
{
    private static final String PREFIX = "fallout.configuration.management.nosqlbench.";
    private static final String NAME = "nosqlbench";
    private static final String DESCRIPTION = "Configures a kubernetes deployment of pods running DS Bench.";
    private static final String DEFAULT_IMAGE = "nosqlbench/nosqlbench:latest";

    public NoSqlBenchConfigurationManager()
    {
        super(PREFIX, NAME, DESCRIPTION, DEFAULT_IMAGE);
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        return ImmutableSet.<Class<? extends Provider>>builder()
            .addAll(super.getRequiredProviders(nodeGroupProperties))
            .add(DataStaxCassOperatorProvider.class)
            .build();
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(NoSqlBenchProvider.class);
    }

    @Override
    public boolean registerProviders(Node node)
    {
        return executeIfNodeHasPod(node, "register nosqlbench provider", (n, p) -> {
            new NoSqlBenchProvider(node, maybeGetNamespace(), getPodArtifactsDir(), getPodLabelSelector());
            return true;
        }).orElse(true);
    }
}
