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
package com.datastax.fallout.components.common.checker;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;

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
        PropertySpecBuilder
            .<Predicate<String>>create(prefix)
            .name("processes")
            .description("Comma-separated list of processes to check")
            .parser(input -> x -> List.of(((String) input).split(",")).contains(x))
            .defaultOf(x -> true)
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
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(processFilterSpec);
    }

    @Override
    public boolean checkHistory(Ensemble ensemble, Collection<Operation> history)
    {
        List<Operation> failedOperations = history.stream()
            .filter(op -> processFilterSpec.value(this).test(op.getProcess()))
            .filter(op -> op.getType() == Operation.Type.fail)
            .collect(Collectors.toList());
        if (failedOperations.isEmpty())
        {
            logger().info("History in jepsen-history.json contained no failures");
            return true;
        }
        else
        {
            String errorMsg = "History in jepsen-history.json contained " + failedOperations.size() + " failures:";
            errorMsg += "\nFirst failure: " + failedOperations.get(0);
            if (failedOperations.size() > 1)
            {
                errorMsg += "\nLast failure: " + failedOperations.get(failedOperations.size() - 1);
            }
            logger().error(errorMsg);
            return false;
        }
    }
}
