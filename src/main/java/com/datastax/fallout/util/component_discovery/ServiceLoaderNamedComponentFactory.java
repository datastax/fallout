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
import java.util.List;

import com.datastax.fallout.ops.PropertyBasedComponent;
import com.datastax.fallout.ops.Utils;

public class ServiceLoaderNamedComponentFactory<T extends PropertyBasedComponent> implements NamedComponentFactory<T>
{
    private final List<T> loadedComponents;

    public ServiceLoaderNamedComponentFactory(Class<T> clazz)
    {
        loadedComponents = Utils.loadComponents(clazz);
    }

    @Override
    public T createComponent(String name)
    {
        for (T aT : loadedComponents)
        {
            try
            {
                if (aT.name().equalsIgnoreCase(name))
                    return (T) aT.getClass().getDeclaredConstructor().newInstance();
            }
            catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
                InvocationTargetException e)
            {
                throw new RuntimeException("Error creating instance", e);
            }
        }
        return null;
    }
}
