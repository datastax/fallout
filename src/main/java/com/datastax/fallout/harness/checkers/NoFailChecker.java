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
import java.util.function.Predicate;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * A Checker that validates a history by checking for any Operations with type Type.fail.
 *
 * This will not check for incomplete operations (represented by Type.info returned after a Type.invoke operation).
 */

@AutoService(Checker.class)
public class NoFailChecker extends Checker
{
    static final String prefix = "fallout.checkers.nofail.";

    static final PropertySpec<Predicate<String>> processFilterSpec =
        PropertySpecBuilder.create(prefix)
            .name("processes")
            .description("Comma-separated list of processes to check")
            .parser(input -> (Predicate) (x -> Arrays.asList(((String) input).split(",")).contains(x)))
            .defaultOf(PropertySpec.Value.of("all", (Predicate) x -> true, ""))
            .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "nofail";
    }

    @Override
    public String description()
    {
        return "Checks that no :fail operations are present in the history";
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(processFilterSpec)
            .build();
    }

    @Override
    public boolean check(Ensemble ensemble, Collection<Operation> history)
    {
        Boolean noFail = history.stream()
            .filter(op -> processFilterSpec.value(this).test(op.getProcess()))
            .allMatch(op -> op.getType() != Operation.Type.fail);
        if (noFail)
        {
            logger.info("History contained no failures");
        }
        else
        {
            logger.error("History contained failures: see jepsen-history.json");
        }
        return noFail;
    }
}
