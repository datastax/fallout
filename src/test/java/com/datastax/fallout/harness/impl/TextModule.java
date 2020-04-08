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

import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;

@AutoService(Module.class)
public class TextModule extends Module
{
    static final String prefix = "test.module.text.";

    static final PropertySpec<String> textSpec =
        PropertySpecBuilder.createStr(prefix).name("text")
            .description("The text to print")
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
        return "text";
    }

    @Override
    public String description()
    {
        return "A module that emits text. Used for checking order of modules.";
    }

    @Override
    public Optional<String> exampleUsage()
    {
        return Optional.of("9-nested-subphases.yaml");
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(textSpec)
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
        emitOk(textSpec.value(properties));
    }
}
