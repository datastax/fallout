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
package com.datastax.fallout.components.common.module;

import java.util.List;

import com.google.auto.service.AutoService;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * A module provided for testing that transitions
 * an entire nodegroup to a different runlevel
 */
@AutoService(Module.class)
public class RunlevelModule extends Module
{
    static final String prefix = "fallout.module.runlevel.";

    static final PropertySpec<Ensemble.Role> roleSpec = PropertySpecBuilder.createEnum(prefix, Ensemble.Role.class)
        .name("role")
        .description("The role to transition to the new runlevel")
        .required()
        .build();

    static final PropertySpec<String> nodeGroupSpec = PropertySpecBuilder.nodeGroup(prefix);

    static final PropertySpec<NodeGroup.State> runlevelSpec = PropertySpecBuilder
        .createEnum(prefix, NodeGroup.State.class)
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
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return List.of(runlevelSpec,
            roleSpec,
            nodeGroupSpec);
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
            logger().error("Trying to transition down from {} to {}. This operation will destroy test artifacts" +
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
                logger().error("Failed to transition with error {}", e);
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
