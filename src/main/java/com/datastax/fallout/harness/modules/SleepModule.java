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
package com.datastax.fallout.harness.modules;

import java.util.List;
import java.util.Set;

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
import com.datastax.fallout.util.Duration;

/**
 * SleepModule allowing fallout test to sleep for given duration Useful if we want the situation to stabilize,
 * workaround a bug or gather JFR in a quiet period
 */
@AutoService(Module.class)
public class SleepModule extends Module
{
    public static final String NAME = "sleep";

    static final String PREFIX = String.format("fallout.module.%s.", NAME);

    private static final PropertySpec<Duration> sleepDurationSpec = PropertySpecBuilder
        .createDuration(PREFIX)
        .name("duration")
        .description("How long to sleep e.g. 10m, 13s")
        .defaultOf(Duration.minutes(1))
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
        return "Sleeps for given duration";
    }

    @Override
    public List<PropertySpec> getModulePropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(sleepDurationSpec)
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
        emit(Operation.Type.invoke);

        Duration sleepDuration = sleepDurationSpec.value(properties);
        try
        {
            Thread.sleep(sleepDuration.toMillis());
            emit(Operation.Type.ok);
        }
        catch (InterruptedException e)
        {
            emit(Operation.Type.fail);
        }
    }
}
