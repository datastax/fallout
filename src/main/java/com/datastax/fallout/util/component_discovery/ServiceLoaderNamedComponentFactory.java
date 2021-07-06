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

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Uses {@link ServiceLoader} to load all instances of a particular Component class; components with
 *  the same name are allowed, the conflictResolver constructor parameter being used to choose between
 *  them, by using the maximum of the two components according to the comparison it implements. */
public class ServiceLoaderNamedComponentFactory<Component extends NamedComponent>
    implements NamedComponentFactory<Component>
{
    private static final Logger log = LoggerFactory.getLogger(ServiceLoaderNamedComponentFactory.class);
    private final Map<String, Component> loadedComponents;

    public ServiceLoaderNamedComponentFactory(Class<Component> clazz)
    {
        this(clazz, Comparator.comparing(component -> component.getClass().getName()));
    }

    public ServiceLoaderNamedComponentFactory(Class<Component> clazz, Comparator<NamedComponent> conflictResolver)
    {
        loadedComponents = loadComponents(clazz, conflictResolver);
    }

    public static <Component extends NamedComponent> Collection<Component>
        loadComponents(Class<Component> componentClass)
    {
        return loadComponents(componentClass, Comparator.comparing(component -> component.getClass().getName()))
            .values();
    }

    private static <Component extends NamedComponent> Map<String, Component>
        loadComponents(Class<Component> componentClass, Comparator<NamedComponent> conflictResolver)
    {
        try
        {
            ServiceLoader<Component> loadedComponents = ServiceLoader.load(componentClass);
            return StreamSupport
                .stream(loadedComponents.spliterator(), false)
                // Create a map of name -> component, handling components with the same name
                // using conflictResolver to decide which to keep
                .collect(Collectors.toMap(
                    component -> component.name().toLowerCase(Locale.ROOT),
                    Function.identity(),
                    (a, b) -> conflictResolver.compare(a, b) > 0 ? a : b,
                    // Use a TreeMap to keep the components sorted by name
                    TreeMap::new));
        }
        catch (Throwable t)
        {
            log.error("Failed to loadComponents for " + componentClass, t);
            throw t;
        }
    }

    @SuppressWarnings("unchecked")
    private Component createNewInstance(Component componentInstance)
    {
        try
        {
            return (Component) componentInstance.getClass().getDeclaredConstructor().newInstance();
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
            InvocationTargetException e)
        {
            throw new RuntimeException("Error creating instance", e);
        }
    }

    @Override
    public Component create(String name)
    {
        final var componentInstance = loadedComponents.get(name.toLowerCase(Locale.ROOT));
        return componentInstance != null ? createNewInstance(componentInstance) : null;
    }

    /** Return components sorted by name */
    @Override
    public Collection<Component> exampleComponents()
    {
        return loadedComponents.values();
    }
}
