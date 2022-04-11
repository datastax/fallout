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

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
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
        }

        public List<PropertySpec<?>> getSpecs()
        {
            return List.of(providerClassSpec, providerArgsSpec);
        }

        public boolean registerDynamicProvider(Node node, PropertyGroup properties, ScopedLogger logger)
        {
            String providerClassName = providerClassSpec.value(properties);
            List<String> providerArgs = providerArgsSpec.value(properties);

            if (providerClassName == null)
            {
                return true;
            }

            Class clazz;
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
                Class argTypes[] = new Class[numArgs + 1];
                Object args[] = new Object[argTypes.length];

                argTypes[0] = Node.class;
                args[0] = node;

                for (int i = 1; i < argTypes.length; i++)
                {
                    argTypes[i] = String.class;
                    args[i] = providerArgs.get(i - 1);
                }

                clazz.getConstructor(argTypes).newInstance(args);
                return true;
            }
            catch (NoSuchMethodException e)
            {
                logger.error("No Provider class {} constructor with {} String args found", providerClassName,
                    providerArgs == null ? 0 : providerArgs.size());
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

            Class clazz;
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

            try
            {
                Class argTypes[] = new Class[numArgs + 1];
                Arrays.fill(argTypes, String.class);
                argTypes[0] = Node.class;

                clazz.getConstructor(argTypes);

                return Set.of(clazz, HelmProvider.class);
            }
            catch (NoSuchMethodException e)
            {
                throw new RuntimeException("Specified provider class has no constructor with " + numArgs +
                    " String arguments: " + providerClassName);
            }
        }
    }
}
