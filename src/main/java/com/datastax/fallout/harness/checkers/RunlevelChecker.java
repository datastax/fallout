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
package com.datastax.fallout.harness.checkers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.harness.modules.RunlevelModule;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * Checks whether runlevels on a history fit a given sequence
 */
@AutoService(Checker.class)
public class RunlevelChecker extends Checker
{
    private static final String prefix = "test.checkers.";

    static final PropertySpec<List<NodeGroup.State>> runlevelsSpec =
        PropertySpecBuilder.create(prefix)
            .name("runlevels")
            .description("comma separated list of runlevels to find on the history")
            .parser(input -> Arrays.asList(((String) input).split(","))
                .stream().map(x -> NodeGroup.State.valueOf(x.trim()))
                .collect(Collectors.toList()))
            .required()
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
        return "Checks whether runlevels on a history fit a given sequence";
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(runlevelsSpec)
            .build();
    }

    public boolean check(Ensemble ensemble, Collection<Operation> history)
    {
        List<NodeGroup.State> actualRunlevels = history.stream()
            .filter(op -> op.getModule() instanceof RunlevelModule)
            .filter(op -> op.getType().equals(Operation.Type.ok))
            .map(op -> (NodeGroup.State) op.getValue())
            .collect(Collectors.toList());

        List<NodeGroup.State> expectedRunlevels = runlevelsSpec.value(this);

        logger().info("Expected runlevels {}", expectedRunlevels);
        logger().info("Actual runlevels {}", actualRunlevels);

        return actualRunlevels.equals(expectedRunlevels);
    }
}
