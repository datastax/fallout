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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;

/**
 * Module used for testing emit from multiple threads
 */
@AutoService(Module.class)
public class MultithreadedEmitModule extends Module
{
    static final String prefix = "test.module.multithreadedemit.";

    static final PropertySpec<Integer> numEmitsSpec =
        PropertySpecBuilder.createInt(prefix).name("emit.count")
            .description("The number of emits to perform off the main thread")
            .defaultOf(10)
            .build();

    public MultithreadedEmitModule()
    {
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "multithreaded";
    }

    @Override
    public String description()
    {
        return "A module that emits from multiple threads";
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(numEmitsSpec)
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
        int emitCount = numEmitsSpec.value(properties);
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch latch = new CountDownLatch(emitCount);

        emit(Operation.Type.invoke);

        for (int i = 0; i < emitCount; i++)
        {
            executorService.submit(() -> {
                emit(Operation.Type.info);
                latch.countDown();
            });
        }

        try
        {
            if (latch.await(20, TimeUnit.SECONDS))
            {
                emit(Operation.Type.ok);
            }
            else
            {
                emit(Operation.Type.fail, "Latch timeout expired");
            }
        }
        catch (InterruptedException e)
        {
            emit(Operation.Type.fail, "Latch interrupted");
        }

        executorService.shutdown();
    }
}
