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
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.datastax.fallout.ops.PropertyBasedComponent;

public class MockingComponentFactory implements ComponentFactory
{
    private final Multimap<Class<?>, NamedComponentFactory<?>> factories = HashMultimap.create();
    private final ComponentFactory delegate = new ServiceLoaderComponentFactory();

    private static class MockingNamedComponentFactory<Component extends PropertyBasedComponent>
        implements NamedComponentFactory<Component>
    {
        private final String name;
        private final Supplier<Component> factory;

        private MockingNamedComponentFactory(String name, Supplier<Component> factory)
        {
            this.name = name;
            this.factory = factory;
        }

        @Override
        public Component create(String name)
        {
            return this.name == null || this.name.equals(name) ?
                factory.get() :
                null;
        }

        @Override
        public Collection<Component> exampleComponents()
        {
            return List.of();
        }
    }

    public MockingComponentFactory clear()
    {
        factories.clear();
        return this;
    }

    public <Component extends PropertyBasedComponent> MockingComponentFactory mockAll(
        Class<Component> clazz, Supplier<Component> factory)
    {
        factories.put(clazz, new MockingNamedComponentFactory<>(null, factory));
        return this;
    }

    public <Component extends PropertyBasedComponent> MockingComponentFactory mockNamed(
        Class<Component> clazz, String name, Supplier<Component> factory)
    {
        factories.put(clazz, new MockingNamedComponentFactory<>(name, factory));
        return this;
    }

    @Override
    public <Component extends NamedComponent> Component create(Class<Component> clazz, String name)
    {
        final var factories = this.factories.get(clazz);

        return factories.stream()
            .map(factory -> clazz.cast(factory.create(name)))
            .filter(Objects::nonNull)
            .findFirst()
            .orElseGet(() -> delegate.create(clazz, name));
    }

    @Override
    public <Component extends NamedComponent> Collection<Component> exampleComponents(Class<Component> clazz)
    {
        return delegate.exampleComponents(clazz);
    }
}
