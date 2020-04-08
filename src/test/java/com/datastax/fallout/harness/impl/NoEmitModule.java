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
import java.util.Set;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;

/**
 * Module for testing - it emits no events, so an error should be added to the history automatically.
 */
@AutoService(Module.class)
public class NoEmitModule extends Module
{
    @Override
    public String prefix()
    {
        return "test.module.noemit.";
    }

    @Override
    public String name()
    {
        return "noemit";
    }

    @Override
    public String description()
    {
        return "A module that emits no operations";
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
    }
}
