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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class ServiceLoaderComponentFactory implements ComponentFactory
{
    private Map<Class<?>, ServiceLoaderNamedComponentFactory<?>> factories = new HashMap<>();

    private static final Comparator<NamedComponent> DEFAULT_CONFLICT_RESOLVER = Comparator
        .comparing((NamedComponent component) -> component.getClass().getName())
        .reversed();

    private final Comparator<NamedComponent> conflictResolver;

    /** Create a ServiceLoaderComponentFactory that resolves name conflicts using {@link Class#getName} */
    public ServiceLoaderComponentFactory()
    {
        this.conflictResolver = DEFAULT_CONFLICT_RESOLVER;
    }

    /** Create a ServiceLoaderComponentFactory that resolves name conflicts
     *  using the specified resolver, falling back to {@link Class#getName} */
    public ServiceLoaderComponentFactory(Comparator<NamedComponent> conflictResolver)
    {
        this.conflictResolver = conflictResolver.thenComparing(DEFAULT_CONFLICT_RESOLVER);
    }

    @SuppressWarnings("unchecked")
    private <Component extends NamedComponent>
        ServiceLoaderNamedComponentFactory<Component> getNamedComponentFactory(Class<Component> clazz)
    {
        return (ServiceLoaderNamedComponentFactory<Component>) factories
            .computeIfAbsent(clazz, ignored -> new ServiceLoaderNamedComponentFactory<>(clazz, conflictResolver));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Component extends NamedComponent> Component create(Class<Component> clazz, String name)
    {
        return getNamedComponentFactory(clazz).create(name);
    }

    @Override
    public <Component extends NamedComponent> Collection<Component> exampleComponents(Class<Component> clazz)
    {
        return getNamedComponentFactory(clazz).exampleComponents();
    }
}
