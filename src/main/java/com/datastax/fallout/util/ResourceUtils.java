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

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.jar.JarFile;

import com.google.common.io.Resources;

public class ResourceUtils
{
    private ResourceUtils()
    {
        // utility class
    }

    private static Optional<URL> maybeGetResource(Class<?> contextClass, String resourceName)
    {
        return Optional.ofNullable(contextClass.getResource(resourceName));
    }

    @SuppressWarnings("UnstableApiUsage")
    public static URL getResource(Class<?> contextClass, String resourceName)
    {
        return Resources.getResource(contextClass, resourceName);
    }

    @SuppressWarnings("UnstableApiUsage")
    private static byte[] getResourceAsBytes(URL resourceUrl)
    {
        return Exceptions.getUncheckedIO(() -> Resources.toByteArray(resourceUrl));
    }

    @SuppressWarnings("UnstableApiUsage")
    private static String getResourceAsString(URL resourceUrl)
    {
        return Exceptions.getUncheckedIO(() -> Resources.toString(resourceUrl, StandardCharsets.UTF_8));
    }

    public static Optional<String> maybeGetResourceAsString(Class<?> contextClass, String resourceName)
    {
        return maybeGetResource(contextClass, resourceName)
            .map(ResourceUtils::getResourceAsString);
    }

    public static Optional<String> maybeGetResourceAsString(Object context, String resourceName)
    {
        return maybeGetResourceAsString(context.getClass(), resourceName);
    }

    public static String getResourceAsString(Class<?> contextClass, String resourceName)
    {
        return getResourceAsString(getResource(contextClass, resourceName));
    }

    public static String getResourceAsString(Object context, String resourceName)
    {
        return getResourceAsString(context.getClass(), resourceName);
    }

    private static void walkJarResourceTree(String path, URL resourceUrl, Consumer<String> pathConsumer)
    {
        final var jarAndResourcePath = resourceUrl.getPath().split("!", 2);
        final var jarPath = jarAndResourcePath[0].substring(5);
        final var resourcePath = jarAndResourcePath[1].substring(1);

        final var jarFile = Exceptions.getUncheckedIO(() -> new JarFile(new File(jarPath)));
        final var entries = jarFile.entries();

        while (entries.hasMoreElements())
        {
            final var entry = entries.nextElement();
            final var entryName = entry.getName();
            if (!entry.isDirectory() && entryName.startsWith(resourcePath))
            {
                final var entryPath = path + entryName.substring(resourcePath.length());
                pathConsumer.accept(entryPath);
            }
        }
        Exceptions.runUncheckedIO(jarFile::close);
    }

    private static void walkFileResourceTree(String path, File resourceFile, Consumer<String> pathConsumer)
    {
        if (resourceFile.isDirectory())
        {
            for (var file : resourceFile.listFiles())
            {
                walkFileResourceTree(path + "/" + file.getName(), file, pathConsumer);
            }
        }
        else
        {
            pathConsumer.accept(path);
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    public static void walkResourceTree(Class<?> clazz, String path, BiConsumer<String, byte[]> pathAndContentConsumer)
    {
        final Consumer<String> pathConsumer = path_ -> pathAndContentConsumer.accept(path_,
            getResourceAsBytes(Resources.getResource(clazz, path_)));

        final var resourceUrl = clazz.getResource(path);
        if (resourceUrl.getProtocol().equals("jar"))
        {
            walkJarResourceTree(path, resourceUrl, pathConsumer);
        }
        else
        {
            walkFileResourceTree(path, new File(Exceptions.getUnchecked(() -> resourceUrl.toURI())), pathConsumer);
        }
    }
}
