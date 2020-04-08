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
import java.util.concurrent.atomic.AtomicInteger;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provider;

/**
 * A module used to test lifecycle behavior of the Jepsen harness by counting how many instances of this module have been set up without being torn down (i.e., are "live").
 */
@AutoService(Module.class)
public class LiveCountModule extends Module
{
    private static AtomicInteger liveCount = new AtomicInteger(0);

    @Override
    public String prefix()
    {
        return "test.module.livecount.";
    }

    @Override
    public String name()
    {
        return "count";
    }

    @Override
    public String description()
    {
        return "A module that returns the number of instances that are currently set up without being torndown.";
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
    public void setup(Ensemble ensemble, PropertyGroup properties)
    {
        liveCount.incrementAndGet();
    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        emit(Operation.Type.invoke);
        emit(Operation.Type.ok, liveCount.get());
    }

    @Override
    public void teardown(Ensemble ensemble, PropertyGroup properties)
    {
        liveCount.decrementAndGet();
    }
}
