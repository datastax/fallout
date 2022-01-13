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
package com.datastax.fallout.components.nosqlbench;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.kubernetes.KubernetesDeploymentConfigurationManager;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;

@AutoService(ConfigurationManager.class)
public class NoSqlBenchConfigurationManager extends KubernetesDeploymentConfigurationManager.WithPersistentContainer
{
    private static final String PREFIX = "fallout.configuration.management.nosqlbench.";
    private static final String NAME = "nosqlbench";
    private static final String DESCRIPTION = "Configures a kubernetes deployment of pods running NoSQLBench.";
    private static final String DEFAULT_IMAGE = "nosqlbench/nosqlbench:latest";

    /**
     * These resources must be declared manually and upfront in order for them to be deployed as part of the NoSqlBench
     * pod. Without this, all managed files from the namespace where NoSqlBench is deployed would have to be mounted to
     * the NoSqlBench pods. The lookup process for all managed files within a namespace adds unnecessary complexity,
     * requiring a transformation from RemoteFileSpec -> file reference string -> RemoteManagedFileRef.
     *
     * If a simpler lookup method is found or the need for this configuration manager is removed (e.g. by making it
     * possible to deploy NoSqlBench as a Kubernetes job) this property spec can be deprecated and it's value safely
     * ignored.
     */
    private final PropertySpec<List<FileProvider.RemoteManagedFileRef>> resourcesSpec =
        PropertySpecBuilder.createRemoteManagedFileRefList(PREFIX)
            .name("nosqlbench.resources")
            .description("List of remote managed file references to include with the NoSqlBench deployment")
            .build();

    public NoSqlBenchConfigurationManager()
    {
        super(PREFIX, NAME, DESCRIPTION, DEFAULT_IMAGE);
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(NoSqlBenchPodProvider.class);
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup nodeGroupProperties)
    {
        var required = new HashSet<>(super.getRequiredProviders(nodeGroupProperties));
        resourcesSpec.optionalValue(nodeGroupProperties)
            .ifPresent(ignored -> required.add(FileProvider.RemoteFileProvider.class));
        return required;
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("kubernetes/nosqlbench-workload.yaml");
    }

    @Override
    public boolean registerProviders(Node node)
    {
        return executeIfNodeHasPod(node, "register nosqlbench provider", (n, p) -> {
            new NoSqlBenchPodProvider(node, maybeGetNamespace(), getPodArtifactsDir(), getPodLabelSelector());
            return true;
        }).orElse(true);
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        var specs = new ArrayList<>(super.getPropertySpecs());
        specs.add(resourcesSpec);
        return specs;
    }

    private Stream<FileProvider.RemoteManagedFileRef> getRequiredFiles()
    {
        return resourcesSpec.optionalValue(getNodeGroup()).stream().flatMap(Collection::stream);
    }

    @Override
    public List<Map<String, String>> getConfigMapVolumes()
    {
        return getRequiredFiles()
            .map(remoteFile -> Map.of(
                "name", remoteFile.fileName(getNodeGroup()),
                "configMap", remoteFile.fileName(getNodeGroup())))
            .toList();
    }

    @Override
    public List<Map<String, String>> getVolumeMounts()
    {
        return getRequiredFiles()
            .map(remoteFile -> Map.of(
                "name", remoteFile.fileName(getNodeGroup()),
                // Managed file must be mounted at parent path because kubernetes will
                // create a file per ConfigMap key under this directory.
                "mountPath", remoteFile.fullPath(getNodeGroup()).getParent().toString()
            ))
            .toList();
    }
}
