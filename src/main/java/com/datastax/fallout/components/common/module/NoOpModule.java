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
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

@AutoService(Module.class)
public class NoOpModule extends Module
{
    public static final String NAME = "noop";

    static final String PREFIX = String.format("fallout.module.%s.", NAME);

    static final PropertySpec<List<String>> propertiesSpec = PropertySpecBuilder.createStrList(PREFIX)
        .name("properties")
        .description("A list of properties that will be printed")
        .build();

    @Override
    public String prefix()
    {
        return PREFIX;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public String description()
    {
        return "A module that does nothing except print its properties";
    }

    @Override
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return List.of(propertiesSpec);
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        emitInvoke((String.format("Starting %s module: %s", NAME, properties)));
        emitOk(String.format("Success: %s succeeded!", NAME));
    }
}
