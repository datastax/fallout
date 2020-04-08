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
package com.datastax.fallout.harness.modules;

import java.util.List;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;

/**
 * A module provided for testing that transitions
 * an entire nodegroup to a different runlevel
 */
@AutoService(Module.class)
public class RunlevelModule extends Module
{
    static final String prefix = "fallout.module.runlevel.";

    static final PropertySpec<Ensemble.Role> roleSpec =
        PropertySpecBuilder.create(prefix)
            .name("role")
            .description("The role to transition to the new runlevel")
            .optionsArray(Ensemble.Role.values())
            .required()
            .build();

    static final PropertySpec<String> nodeGroupSpec = PropertySpecBuilder.nodeGroup(prefix);

    static final PropertySpec<NodeGroup.State> runlevelSpec =
        PropertySpecBuilder.create(prefix)
            .name("runlevel")
            .description("The target runlevel")
            .options(NodeGroup.State.DESTROYED,
                NodeGroup.State.STOPPED,
                NodeGroup.State.STARTED_SERVICES_UNCONFIGURED,
                NodeGroup.State.STARTED_SERVICES_CONFIGURED,
                NodeGroup.State.STARTED_SERVICES_RUNNING)
            .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "runlevel";
    }

    @Override
    public String description()
    {
        return "A module that transitions a role to a runlevel";
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(runlevelSpec,
                roleSpec,
                nodeGroupSpec)
            .build();
    }

    @Override
    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        return ImmutableSet.of();
    }

    @Override
    public List<Product> getSupportedProducts()
    {
        return Product.everything();
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        emit(Operation.Type.invoke);
        NodeGroup targetGroup = ensemble.getGroup(roleSpec.value(properties), nodeGroupSpec.value(properties));
        NodeGroup.State stateToTransitionTo = runlevelSpec.value(properties);

        boolean transitioned = false;

        if (targetGroup.getState().ordinal() > NodeGroup.State.STARTED_SERVICES_UNCONFIGURED.ordinal() &&
            stateToTransitionTo.ordinal() <= NodeGroup.State.STARTED_SERVICES_UNCONFIGURED.ordinal())
        {
            logger.error("Trying to transition down from {} to {}. This operation will destroy test artifacts" +
                "and will not be allowed.", targetGroup.getState(), stateToTransitionTo);
        }
        else
        {
            try
            {
                transitioned = targetGroup.transitionState(stateToTransitionTo).join().wasSuccessful();
            }
            catch (Exception e)
            {
                logger.error("Failed to transition with error {}", e);
            }
        }

        if (transitioned)
        {
            emit(Operation.Type.ok, targetGroup.getState());
        }
        else
        {
            emit(Operation.Type.fail);
        }
    }
}
