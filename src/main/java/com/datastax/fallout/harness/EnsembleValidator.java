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
package com.datastax.fallout.harness;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.common.spec.NodeSelectionSpec;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyBasedComponent;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.Provider;

/**
 * Contains validation methods for common Ensemble configuration requirements
 */
public class EnsembleValidator
{
    private final PropertyBasedComponent component;
    private final ActiveTestRunBuilder.ValidationResult validationResult;
    private final Ensemble ensemble;
    private final PropertyGroup properties;

    EnsembleValidator(PropertyBasedComponent component, PropertyGroup properties, Ensemble ensemble,
        ActiveTestRunBuilder.ValidationResult validationResult)
    {
        this.component = component;
        this.validationResult = validationResult;
        this.ensemble = ensemble;
        this.properties = properties;
    }

    public Ensemble getEnsemble()
    {
        return ensemble;
    }

    public Optional<NodeGroup> getNodeGroup(NodeSelectionSpec nodeSelection)
    {
        return getNodeGroup(nodeSelection.getNodeGroupSpec());
    }

    public Optional<NodeGroup> getNodeGroup(PropertySpec<String> nodeGroupSpec)
    {
        String nodeGroupAlias = nodeGroupSpec.value(properties);
        return ensemble.maybeGetNodeGroupByAlias(nodeGroupAlias);
    }

    private void doWithValidNodeGroup(PropertySpec<String> nodeGroupSpec, BiConsumer<String, NodeGroup> func)
    {
        Optional<NodeGroup> nodeGroup = getNodeGroup(nodeGroupSpec);
        if (nodeGroup.isEmpty())
        {
            failNodeGroupSelection(nodeGroupSpec);
            return;
        }
        func.accept(nodeGroupSpec.shortName(), nodeGroup.get());
    }

    public void addValidationError(String reason)
    {
        String prefix = String.format("%s '%s'", component.getClass().getSimpleName(), component.getInstanceName());
        validationResult.addError(String.format("%s failed validation: %s",
            prefix, reason));
    }

    private void requireProvider(String selectingProp, NodeGroup nodeGroup, Class<? extends Provider> provider)
    {
        if (!nodeGroup.willHaveProvider(provider))
        {
            addValidationError(
                String.format("NodeGroup '%s' (selected by '%s') is missing a %s", nodeGroup.getId(), selectingProp,
                    provider.getSimpleName()));
        }
    }

    private void failNodeGroupSelection(PropertySpec<String> nodeGroupSpec)
    {
        String nodeGroupAlias = nodeGroupSpec.value(properties);
        addValidationError(String
            .format("NodeGroup with alias '%s' is missing (selected by property: %s)", nodeGroupAlias,
                nodeGroupSpec.shortName()));
    }

    public void requireNodeGroup(PropertySpec<String> nodeGroupSpec)
    {
        doWithValidNodeGroup(nodeGroupSpec, (selectingProp, nodeGroup) -> {
            // we only call this to make sure the nodegroup exists
        });
    }

    private void nodeGroupRequiresProviders(String selectingProp, NodeGroup nodeGroup,
        Set<Class<? extends Provider>> providerClasses)
    {
        for (Class<? extends Provider> reqProvider : providerClasses)
        {
            requireProvider(selectingProp, nodeGroup, reqProvider);
        }
    }

    public void nodeGroupRequiresProviders(PropertySpec<String> nodeGroupSpec,
        Set<Class<? extends Provider>> providerClasses)
    {
        doWithValidNodeGroup(nodeGroupSpec, (selectingProp, nodeGroup) -> {
            nodeGroupRequiresProviders(selectingProp, nodeGroup, providerClasses);
        });
    }

    public void nodeGroupRequiresProvider(NodeSelectionSpec nodeSelection, Class<? extends Provider> providerClass)
    {
        nodeGroupRequiresProvider(nodeSelection.getNodeGroupSpec(), providerClass);
    }

    public void nodeGroupRequiresProvider(PropertySpec<String> nodeGroupSpec,
        Class<? extends Provider> providerClass)
    {
        nodeGroupRequiresProviders(nodeGroupSpec, Set.of(providerClass));
    }

    @SafeVarargs
    public final void nodeGroupRequiresExactlyOneProvider(PropertySpec<String> nodeGroupSpec,
        Class<? extends Provider>... providerClasses)
    {
        doWithValidNodeGroup(nodeGroupSpec, (selectingProp, nodeGroup) -> {
            var satisfiedProviders = Arrays.stream(providerClasses)
                .filter(nodeGroup::willHaveProvider)
                .collect(Collectors.toSet());
            if (satisfiedProviders.size() != 1)
            {
                addValidationError(String.format(
                    "NodeGroup '%s' (selected by '%s') required exactly one of %s but found %s",
                    nodeGroup.getId(), selectingProp, Arrays.toString(providerClasses), satisfiedProviders));
            }
        });
    }

    public boolean nodeGroupWillHaveProvider(NodeSelectionSpec nodeSelection,
        Class<? extends Provider> providerClass)
    {
        return nodeGroupWillHaveProvider(nodeSelection.getNodeGroupSpec(), providerClass);
    }

    public boolean nodeGroupWillHaveProvider(PropertySpec<String> nodeGroupSpec,
        Class<? extends Provider> providerClass)
    {
        return getNodeGroup(nodeGroupSpec).map(ng -> ng.willHaveProvider(providerClass)).orElse(false);
    }

    public void nodeGroupForbidsProvider(PropertySpec<String> nodeGroupSpec,
        Class<? extends Provider> providerClass)
    {
        doWithValidNodeGroup(nodeGroupSpec, (selectingProp, nodeGroup) -> {
            if (nodeGroup.willHaveProvider(providerClass))
            {
                addValidationError(String.format("NodeGroup '%s' (selected by '%s') has a forbidden %s",
                    nodeGroup.getId(), selectingProp, providerClass.getSimpleName()));
            }
        });
    }

    public void managedFileRefRequiresProvider(NodeSelectionSpec nodeSelection,
        PropertySpec<String> managedFileRefSpec,
        Class<? extends FileProvider> fileProvider)
    {
        managedFileRefRequiresProvider(nodeSelection.getNodeGroupSpec(), managedFileRefSpec, fileProvider);
    }

    public void managedFileRefRequiresProvider(PropertySpec<String> nodeGroupSpec,
        PropertySpec<String> managedFileRefSpec,
        Class<? extends FileProvider> fileProvider)
    {
        Optional<String> managedFileRef = managedFileRefSpec.optionalValue(properties);
        doWithValidNodeGroup(nodeGroupSpec, (propSpec, nodeGroup) -> {
            if (managedFileRef.map(FileProvider::isManagedFileRef).orElse(false))
            {
                requireProvider(propSpec, nodeGroup, fileProvider);
            }
        });
    }

    public void localManagedFileRefRequiresProvider(PropertySpec<String> nodeGroup,
        PropertySpec<FileProvider.LocalManagedFileRef> managedFileRefSpec)
    {
        managedFileRefSpec.optionalValue(properties).ifPresent(
            f -> nodeGroupRequiresProvider(nodeGroup, FileProvider.LocalFileProvider.class));
    }

    public void ensembleRequiresProvider(Class<? extends Provider> providerClass)
    {
        // we can not use Ensemble.findFirstProvider here because they providers are not yet
        // on the Nodes at validation time. Only NodeGroup.getAvailableProviders tell us
        // right now what will be available
        for (NodeGroup nodeGroup : ensemble.getUniqueNodeGroupInstances())
        {
            if (nodeGroup.willHaveProvider(providerClass))
            {
                return;
            }
        }
        addValidationError("Ensemble is missing a " + providerClass.getSimpleName());
    }
}
