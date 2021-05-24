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
package com.datastax.fallout.components.kubernetes;

import java.util.List;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.common.module.RepeatableNodeCommandModule;
import com.datastax.fallout.components.common.spec.NodeSelectionSpec;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.commands.NodeResponse;

import static com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec.buildNameSpaceSpec;

@AutoService(Module.class)
public class KubeControlModule extends RepeatableNodeCommandModule<KubeControlProvider>
{
    private static final String prefix = "fallout.module.kubectl.";

    private static final PropertySpec<String> nameSpaceSpec = buildNameSpaceSpec(prefix);

    public KubeControlModule()
    {
        super(KubeControlProvider.class, prefix,
            (_prefix) -> {
                NodeSelectionSpec.NodeSelectionDefaults defaults = new NodeSelectionSpec.NodeSelectionDefaults();
                // by default run against single node, since kubectl commands are nodegroup-wide anyway
                defaults.setOrdinals(Set.of(0));
                PropertySpec<String> nodeGroupSpec = PropertySpecBuilder.nodeGroup(_prefix, "server");
                return new NodeSelectionSpec(_prefix, "target", false, false, nodeGroupSpec, defaults);
            });
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "kubectl";
    }

    @Override
    public String description()
    {
        return "Executes kubectl commands.";
    }

    @Override
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .addAll(super.getModulePropertySpecs())
            .add(nameSpaceSpec).build();
    }

    @Override
    protected String commandDescription()
    {
        return "kubectl command to execute. Exclude 'kubectl' from command.";
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        super.validateEnsemble(validator);
        // we have to select nodes early for validation, the base class will re-select nodes during execution,
        // but the selection shouldnt change then.
        List<Node> targetNodes = getNodesSpec().selectNodes(validator.getEnsemble(), getProperties());
        if (targetNodes.size() != 1 || targetNodes.get(0).getNodeGroupOrdinal() != 0)
        {
            validator.addValidationError("Node selection for kubectl cmd is not supported");
        }
    }

    @Override
    protected void validateCommand(String command)
    {
        Preconditions.checkArgument(!command.toLowerCase().stripLeading().startsWith("kubectl"),
            "Command may not start with 'kubectl'");
    }

    @Override
    protected NodeResponse runCommand(KubeControlProvider kubeCtlProvider, String command)
    {
        return kubeCtlProvider.inNamespace(nameSpaceSpec.optionalValue(getProperties()),
            kubeCtl -> kubeCtl.execute(command));
    }
}
