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
package com.datastax.fallout.util.component_discovery;

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.datastax.fallout.ops.PropertyBasedComponent;

public class MockingComponentFactory implements ComponentFactory
{
    private Multimap<Class, NamedComponentFactory> factories = HashMultimap.create();
    private final ComponentFactory delegate = new ServiceLoaderComponentFactory();

    public <Component extends PropertyBasedComponent> MockingComponentFactory clear()
    {
        factories.clear();
        return this;
    }

    public <Component extends PropertyBasedComponent> MockingComponentFactory mockAll(
        Class<Component> clazz, Supplier<Component> factory)
    {
        factories.put(clazz, name -> factory.get());
        return this;
    }

    public <Component extends PropertyBasedComponent> MockingComponentFactory mockNamed(
        Class<Component> clazz, String name, Supplier<Component> factory)
    {
        factories.put(clazz, name_ -> name_.equals(name) ? factory.get() : null);
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Component extends PropertyBasedComponent> Component create(Class<Component> clazz, String name)
    {
        final Collection<NamedComponentFactory> factories = this.factories.get(clazz);

        return (Component) factories.stream()
            .map(factory -> factory.createComponent(name))
            .filter(Objects::nonNull)
            .findFirst()
            .orElseGet(() -> delegate.create(clazz, name));
    }
}
