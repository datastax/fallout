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
package com.datastax.fallout.components.fallout_system;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;

/**
 * A Checker that fails a history if it includes any errors.
 *
 * This is included in every test by default.
 */
public class NoErrorChecker extends Checker
{
    static final String prefix = "fallout.checkers.noerror";

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "noerror";
    }

    @Override
    public String description()
    {
        return "Checks that no error operations are present in the history";
    }

    @Override
    public boolean checkHistory(Ensemble ensemble, Collection<Operation> history)
    {
        List<Operation> errors = history.stream()
            .filter(op -> op.getType() == Operation.Type.error)
            .collect(Collectors.toList());
        if (errors.isEmpty())
        {
            logger().info("No errors found in history");
            return true;
        }
        else
        {
            logger().error("Errors found in history: ");
            for (Operation error : errors)
            {
                logger().error("Error: {}", error);
            }
            return false;
        }
    }
}
