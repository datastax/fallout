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
package com.datastax.fallout.components.cassandra;

import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.ops.Node;

import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.provisioner.NoRemoteAccessProvisioner;

import com.google.auto.service.AutoService;

import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.utils.ScriptUtils;

@AutoService(ConfigurationManager.class)
public class CqlshConfigurationManager extends ConfigurationManager
{
    private static final String prefix = "fallout.configuration.management.cqlsh.";
    private static final String name = "cqlsh";
    private static final String description = "Configure cqlsh on client nodes";

    private static final PropertySpec<String> versionSpec = PropertySpecBuilder.createStr(prefix)
        .name("version")
        .description("The version of Cqlsh to install")
        .options("cqlsh-6.8", "cqlsh-6.7", "cqlsh-6.0", "cqlsh-5.1", "cqlsh-astra")
        .defaultOf("cqlsh-astra")
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("astra/cqlsh-astra.yaml");
    }

    static public String buildCqlshUrl(String version)
    {
        return String.format("https://downloads.datastax.com/enterprise/%s.tar.gz", version);
    }

    @Override
    public void doSummarizeInfo(InfoConsumer infoConsumer)
    {
        infoConsumer.accept("version", versionSpec.value(getNodeGroup()));
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        NodeGroup nodeGroup = getNodeGroup();
        if (getNodeGroup().getProvisioner() instanceof NoRemoteAccessProvisioner)
        {
            validator.addValidationError("NodeGroup '" + nodeGroup.getId() +
                "' cannot be used since no downloads allowed for provisioner: " +
                nodeGroup.getProvisioner().name());
        }
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        return Set.of(RemoteCqlshProvider.class);
    }

    @Override
    public boolean registerProviders(Node node)
    {
        // See configureImpl for this path: we untar to fallout_library/cqlsh/<version>
        String cqlshPath = String.join(
            "/",
            node.getRemoteLibraryPath(), "cqlsh", versionSpec.value(node.getProperties()), "bin" , "cqlsh");
        new RemoteCqlshProvider(
            node,
            cqlshPath,
            versionSpec.value(node.getProperties()));
        return true;
    }

    @Override
    public boolean unregisterProviders(Node node)
    {
        node.maybeUnregister(RemoteCqlshProvider.class);
        return true;
    }

    @Override
    protected boolean configureImpl(NodeGroup nodeGroup)
    {
        String url = buildCqlshUrl(versionSpec.value(nodeGroup));
        return nodeGroup.waitForNodeSpecificSuccess(node -> {
            return ScriptUtils.script(
                String.format("wget %s -P %s", url, node.getRemoteScratchPath()),
                String.format("mkdir %s/cqlsh", node.getRemoteLibraryPath()),
                String.format("tar -zxvf %s/%s.tar.gz -C %s/cqlsh",
                    node.getRemoteScratchPath(), versionSpec.value(nodeGroup), node.getRemoteLibraryPath())
            );
        });
    }

    @Override
    protected boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return nodeGroup.removeNodeSpecificLibraryDirectories("cqlsh");
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        if (nodeGroup.waitForAllNodes(
            node -> node.existsFile(Paths.get(node.getRemoteLibraryPath(), "cqlsh").toString()),
            "checking if cqlsh is installed"))
        {
            return NodeGroup.State.STARTED_SERVICES_RUNNING;
        }

        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(versionSpec);
    }
}
