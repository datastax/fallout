/*
 * Copyright 2022 DataStax, Inc.
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
package com.datastax.fallout.ops;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.kubernetes.HelmProvider;
import com.datastax.fallout.util.ScopedLogger;

public class ProviderUtil
{
    public static class DynamicProviderSpec
    {
        private final PropertySpec<String> providerClassSpec;
        private final PropertySpec<List<String>> providerArgsSpec;
        private final PropertySpec<Boolean> nodeGroupProviderSpec;

        public DynamicProviderSpec(String prefix)
        {
            this(prefix, () -> prefix);
        }

        public DynamicProviderSpec(String prefix, Supplier<String> runtimePrefix)
        {
            providerClassSpec = PropertySpecBuilder.createStr(prefix)
                .runtimePrefix(runtimePrefix)
                .category("provider")
                .name("provider.class")
                .description("Simple class name of the fallout provider this helm chart adds")
                .suggestions("CassandraContactPointProvider")
                .build();

            providerArgsSpec = PropertySpecBuilder.createStrList(prefix)
                .runtimePrefix(runtimePrefix)
                .category("provider")
                .name("provider.args")
                .description("Options to be passed to the specified provider class constructor")
                .suggestions(ImmutableList.of("9042"))
                .build();

            nodeGroupProviderSpec = PropertySpecBuilder.createBool(prefix)
                .runtimePrefix(runtimePrefix)
                .category("provider")
                .name("provider.is_nodegroup_provider")
                .description("Set true if the provider is a 'nodegroup' provider (only available on node 0)")
                .defaultOf(false)
                .build();
        }

        public List<PropertySpec<?>> getSpecs()
        {
            return List.of(providerClassSpec, providerArgsSpec, nodeGroupProviderSpec);
        }

        public boolean registerDynamicProvider(Node node, PropertyGroup properties, ScopedLogger logger)
        {
            String providerClassName = providerClassSpec.value(properties);
            List<String> providerArgs = providerArgsSpec.value(properties);
            boolean nodeGroupProvider = nodeGroupProviderSpec.value(properties);

            if (providerClassName == null)
            {
                return true;
            }

            if (nodeGroupProvider && node.getNodeGroupOrdinal() != 0)
            {
                return true;
            }

            Class<?> clazz;
            try
            {
                clazz = Provider.class.getClassLoader().loadClass(providerClassName);
            }
            catch (ClassNotFoundException e)
            {
                logger.error("Unable to load provider class: {}", providerClassName);
                return false;
            }

            if (!Provider.class.isAssignableFrom(clazz))
            {
                logger.error("Class {} is found but not a fallout provider class", providerClassName);
                return false;
            }

            int numArgs = providerArgs == null ? 0 : providerArgs.size();

            try
            {
                Constructor<?>[] constructors = clazz.getConstructors();
                for (Constructor<?> constructor : constructors)
                {
                    Class<?>[] argTypes = constructor.getParameterTypes();
                    if (argTypes.length != numArgs + 1)
                    {
                        continue;
                    }
                    if (argTypes[0] != Node.class)
                    {
                        continue;
                    }

                    Object[] args = new Object[argTypes.length];
                    args[0] = node;
                    boolean typesMatch = true;
                    for (int i = 1; i < argTypes.length; i++)
                    {
                        Object convertedArg = convertType(providerArgs.get(i - 1), argTypes[i]);
                        if (!convertedArg.getClass().isAssignableFrom(argTypes[i]))
                        {
                            typesMatch = false;
                            break;
                        }
                        args[i] = convertedArg;
                    }

                    if (typesMatch)
                    {
                        constructor.newInstance(args);
                        return true;
                    }
                }
                logger.error("No matching constructor found for provider class {}", providerClassName);
                return false;
            }
            catch (IllegalAccessException | InstantiationException | InvocationTargetException e)
            {
                logger.error("Error encountered when creating provider class {}", providerClassName, e);
                return false;
            }
        }

        public Set<Class<? extends Provider>> getAvailableDynamicProviders(PropertyGroup properties)
        {
            String providerClassName = providerClassSpec.value(properties);
            List<String> providerArgs = providerArgsSpec.value(properties);

            if (providerClassName == null)
            {
                return Set.of();
            }

            Class<?> clazz;
            try
            {
                clazz = Provider.class.getClassLoader().loadClass(providerClassName);
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Unable to load specified provider class: " + providerClassName);
            }

            if (!Provider.class.isAssignableFrom(clazz))
            {
                throw new RuntimeException(
                    "Specified provider class is found but not a fallout provider class: " + providerClassName);
            }

            int numArgs = providerArgs == null ? 0 : providerArgs.size();

            for (Constructor<?> constructor : clazz.getConstructors())
            {
                Class<?>[] argTypes = constructor.getParameterTypes();
                if (argTypes.length != numArgs + 1)
                {
                    continue;
                }
                if (argTypes[0] != Node.class)
                {
                    continue;
                }
                boolean typesMatch = true;
                for (int i = 1; i < argTypes.length; i++)
                {
                    Object convertedArg = convertType(providerArgs.get(i - 1), argTypes[i]);
                    if (!convertedArg.getClass().isAssignableFrom(argTypes[i]))
                    {
                        typesMatch = false;
                        break;
                    }
                }

                if (typesMatch)
                {
                    return Set.of((Class<? extends Provider>) clazz, HelmProvider.class);
                }
            }

            throw new RuntimeException(
                "Specified provider class has no matching constructor or invalid argument types: " + providerClassName);
        }

        private Object convertType(String value, Class<?> targetType)
        {
            if (targetType == String.class)
                return value;
            if (targetType == Integer.class || targetType == int.class)
                return Integer.parseInt(value);
            if (targetType == Double.class || targetType == double.class)
                return Double.parseDouble(value);
            if (targetType == Boolean.class || targetType == boolean.class)
                return Boolean.parseBoolean(value);
            throw new IllegalArgumentException("Unsupported argument type: " + targetType.getName());
        }
    }
}
