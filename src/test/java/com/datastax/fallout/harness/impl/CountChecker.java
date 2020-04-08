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
package com.datastax.fallout.harness.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.auto.service.AutoService;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * A Checker that counts operations and checks whether they fall within
 * a designated range
 */
@AutoService(Checker.class)
public class CountChecker extends Checker
{
    static final String prefix = "fallout.checkers.count";

    static final PropertySpec<Long> maxCountSpec =
        PropertySpecBuilder.createLong(prefix)
            .name("max")
            .description("Maximum number of operations matching the filters")
            .required()
            .build();

    static final PropertySpec<Long> minCountSpec =
        PropertySpecBuilder.createLong(prefix)
            .name("min")
            .description("Minimum number of operations matching the filters")
            .required()
            .build();

    static final PropertySpec<Predicate<String>> processFilterSpec =
        PropertySpecBuilder.create(prefix)
            .name("processes")
            .description("Comma-separated list of processes to check")
            .parser(input -> (Predicate) (x -> Arrays.asList(((String) input).split(",")).contains(x)))
            .defaultOf((Predicate) x -> true)
            .build();

    static final PropertySpec<Predicate<String>> typeFilterSpec =
        PropertySpecBuilder.create(prefix)
            .name("types")
            .description("Comma-separated list of types to check")
            .parser(input -> (Predicate) (x -> Arrays.asList(((String) input).split(",")).contains(x)))
            .defaultOf((Predicate) x -> true)
            .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "count";
    }

    @Override
    public String description()
    {
        return "Checks that the number of operations matching the filters is in the designated range";
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(maxCountSpec,
                minCountSpec,
                processFilterSpec,
                typeFilterSpec)
            .build();
    }

    @Override
    public boolean check(Ensemble ensemble, Collection<Operation> history)
    {
        Predicate<String> processFilter = processFilterSpec.value(this);
        Predicate<String> typeFilter = typeFilterSpec.value(this);

        long count = history.stream()
            .filter(op -> processFilter.apply(op.getProcess()))
            .filter(op -> typeFilter.apply(op.getType().toString()))
            .count();

        logger.info("Count {}", count);

        return (count <= maxCountSpec.value(this) && (count >= minCountSpec.value(this)));
    }
}
