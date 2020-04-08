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
package com.datastax.fallout.ops;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.IntSupplier;

import com.google.common.base.Preconditions;

import com.datastax.fallout.harness.NullTestRunAbortedStatus;
import com.datastax.fallout.harness.TestRunAbortedStatus;
import com.datastax.fallout.ops.commands.NodeCommandExecutor;
import com.datastax.fallout.util.Exceptions;

/**
 * Builder for NodeGroups. Optional but useful.
 *
 * @see NodeGroup
 */
public class NodeGroupBuilder
{
    private Provisioner provisioner;
    private NodeCommandExecutor nodeCommandExecutor;
    private ConfigurationManager configurationManager;
    private TestRunAbortedStatus testRunAbortedStatus = new NullTestRunAbortedStatus();
    private Ensemble.Role role;
    private Integer nodeCount;
    private WritablePropertyGroup propertyGroup;
    private String name;
    private Set<String> aliases = new HashSet<>();
    private IntSupplier ensembleOrdinalSupplier = EnsembleBuilder.createNodeOrdinalSupplier();
    private JobLoggers loggers = new JobConsoleLoggers();
    private Path testRunArtifactPath;
    private EnsembleCredentials credentials;
    private Optional<NodeGroup.State> finalRunLevel = Optional.of(NodeGroup.State.DESTROYED);
    private LocalFilesHandler localFilesHandler = new LocalFilesHandler(List.of());

    NodeGroup builtNodeGroup = null;

    public static NodeGroupBuilder create()
    {
        return new NodeGroupBuilder();
    }

    public NodeGroupBuilder withName(String name)
    {
        this.name = name;
        withAlias(this.name);
        return this;
    }

    public String getName()
    {
        return this.name;
    }

    public NodeGroupBuilder withAlias(String alias)
    {
        this.aliases.add(alias);
        return this;
    }

    public NodeGroupBuilder withProvisioner(Provisioner provisioner)
    {
        Preconditions.checkArgument(this.builtNodeGroup == null, "nodeGroup already built");
        Preconditions.checkArgument(this.provisioner == null, "provisioner already set");
        this.provisioner = provisioner;

        return this;
    }

    public NodeGroupBuilder withNodeCommandExecutor(NodeCommandExecutor nodeCommandExecutor)
    {
        Preconditions.checkArgument(this.builtNodeGroup == null, "nodeGroup already built");
        Preconditions.checkArgument(this.nodeCommandExecutor == null, "nodeCommandExecutor already set");
        this.nodeCommandExecutor = nodeCommandExecutor;

        return this;
    }

    public NodeGroupBuilder withConfigurationManager(ConfigurationManager configurationManager)
    {
        Preconditions.checkArgument(this.builtNodeGroup == null, "nodeGroup already built");
        Preconditions.checkArgument(this.configurationManager == null, "configuration manager already set");
        this.configurationManager = configurationManager;

        return this;
    }

    public NodeGroupBuilder withTestRunAbortedStatus(TestRunAbortedStatus testRunAbortedStatus)
    {
        this.testRunAbortedStatus = testRunAbortedStatus;
        return this;
    }

    public NodeGroupBuilder withRole(Ensemble.Role role)
    {
        Preconditions.checkArgument(this.builtNodeGroup == null, "nodeGroup already built");

        //In ensembles we can re-use the group for many roles
        //So just keep the first one
        if (this.role == null)
            this.role = role;

        return this;
    }

    public NodeGroupBuilder withNodeCount(int nodeCount)
    {
        Preconditions.checkArgument(this.builtNodeGroup == null, "nodeGroup already built");
        Preconditions.checkArgument(this.nodeCount == null, "node count already set");
        Preconditions.checkArgument(nodeCount > 0 && nodeCount < 1024, "node count out of bounds");

        this.nodeCount = nodeCount;

        return this;
    }

    public NodeGroupBuilder withPropertyGroup(WritablePropertyGroup propertyGroup)
    {
        Preconditions.checkArgument(this.builtNodeGroup == null, "nodeGroup already built");
        Preconditions.checkArgument(this.propertyGroup == null, "property group already set");
        this.propertyGroup = propertyGroup;

        return this;
    }

    public NodeGroupBuilder withEnsembleOrdinalSupplier(IntSupplier ensembleOrdinalSupplier)
    {
        this.ensembleOrdinalSupplier = ensembleOrdinalSupplier;

        return this;
    }

    public NodeGroupBuilder withLoggers(JobLoggers loggers)
    {
        Preconditions.checkArgument(this.builtNodeGroup == null, "nodeGroup already built");
        this.loggers = loggers;

        return this;
    }

    public NodeGroupBuilder withTestRunArtifactPath(Path testRunArtifactPath)
    {
        this.testRunArtifactPath = testRunArtifactPath;
        return this;
    }

    public NodeGroupBuilder withFinalRunLevel(Optional<NodeGroup.State> finalRunLevel)
    {
        this.finalRunLevel = finalRunLevel;
        return this;
    }

    public NodeGroupBuilder withCredentials(EnsembleCredentials credentials)
    {
        this.credentials = credentials;
        return this;
    }

    public NodeGroupBuilder withLocalFilesHandler(LocalFilesHandler localFilesHandler)
    {
        this.localFilesHandler = localFilesHandler;
        return this;
    }

    private void check()
    {
        Preconditions.checkArgument(provisioner != null, "provisioner missing");
        Preconditions.checkArgument(configurationManager != null, "configuration manager missing");
        Preconditions.checkArgument(propertyGroup != null, "property group missing");
        Preconditions.checkArgument(nodeCount != null, "node count missing");
        Preconditions.checkArgument(name != null, "name is missing");
        Preconditions.checkArgument(loggers != null, "loggers is missing");
        Preconditions.checkArgument(testRunArtifactPath != null, "testRunArtifactPath is missing");
    }

    public NodeGroup build()
    {
        check();

        if (builtNodeGroup == null)
        {
            builtNodeGroup = Exceptions.getUnchecked(() -> new NodeGroup(name, aliases, propertyGroup,
                nodeCommandExecutor != null ? nodeCommandExecutor : provisioner,
                provisioner, configurationManager, testRunAbortedStatus, nodeCount, role,
                ensembleOrdinalSupplier, loggers, testRunArtifactPath.resolve(name),
                credentials, finalRunLevel, localFilesHandler));
        }

        return builtNodeGroup;
    }
}
