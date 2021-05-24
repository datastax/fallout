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

import com.google.auto.service.AutoService;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;

/**
 * Simple testing module that, when ran, returns Type.fail. Used to validate that Checkers are correctly handling
 * Type.fail Operations.
 *
 * @see Operation
 */

@AutoService(Module.class)
public class FailModule extends Module
{
    @Override
    public String prefix()
    {
        return "test.module.fail.";
    }

    @Override
    public String name()
    {
        return "fail";
    }

    @Override
    public String description()
    {
        return "A module that always fails";
    }

    @Override
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        emit(Operation.Type.invoke);
        emitFail("Fail module always fails!");
    }

    @Override
    public void teardown(Ensemble ensemble, PropertyGroup properties)
    {

    }
}
