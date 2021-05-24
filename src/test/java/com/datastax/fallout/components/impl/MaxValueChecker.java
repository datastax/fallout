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
package com.datastax.fallout.components.impl;

import java.util.Collection;
import java.util.List;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * A Checker that checkers whether the given number is the maximum value associated with an ok operation in the history..
 */

public class MaxValueChecker extends Checker
{
    static final String prefix = "fallout.checkers.maxinteger";

    static final PropertySpec<Long> maxValueSpec =
        PropertySpecBuilder.createLong(prefix)
            .name("value")
            .description("Maximum value that must be present in the stream")
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
        return "maxvalue";
    }

    @Override
    public String description()
    {
        return "Checks that the given value is both 1) present in the history and 2) the maximum value in the history";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(maxValueSpec);
    }

    @Override
    public boolean checkHistory(Ensemble ensemble, Collection<Operation> history)
    {
        long maxValue = maxValueSpec.value(this);

        logger().info("Using max value " + maxValue);

        return history.stream()
            .filter(op -> op.getType() == Operation.Type.ok)
            .filter(op -> op.getValue() instanceof Number && ((Number) op.getValue()).longValue() <= maxValue)
            .anyMatch(op -> ((Number) op.getValue()).longValue() == maxValue);
    }
}
