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
package com.datastax.fallout.util;

import java.util.HashMap;
import java.util.Map;

import com.datastax.fallout.ops.PropertyBasedComponent;

public class ServiceLoaderComponentFactory implements ComponentFactory
{
    static Map<Class, ServiceLoaderTypedComponentFactory> factories = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <Component extends PropertyBasedComponent> Component create(Class<Component> clazz, String name)
    {
        return ((ServiceLoaderTypedComponentFactory<Component>) factories
            .computeIfAbsent(clazz, ServiceLoaderTypedComponentFactory::new))
                .createComponent(name);
    }
}
