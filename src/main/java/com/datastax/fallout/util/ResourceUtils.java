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
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.jar.JarFile;

/** In addition to some specialized methods, provides a set of consistent
 *  getResource methods, <code>(maybe)getResource(asType)</code>
 *
 *  <ul>
 *      <li>The <code>maybe</code>-prefixed methods return an {@link Optional}, which will be empty if the resource
 *      cannot be found; other methods throw {@link ResourceNotFoundException} if the resource cannot be found</li>
 *      <li>The <code>asType</code>-suffixed methods return the
 *      specified type; other methods return the {@link URL}</li>
 *  </ul>
 *
 *  <p>The <code>resourceName</code> parameters are passed unmodified to the underlying JDK methods
 *     {@link Class#getResource} and {@link Class#getResourceAsStream}; this allows using absolute paths
 *     (i.e. prefixed with <code>/</code>) to access files from the root of the resource hierarchy.
 *
 *  <p>If a method you need isn't here, feel free to add it, following the naming above.
 */
public class ResourceUtils
{
    private ResourceUtils()
    {
        // utility class
    }

    public static class ResourceNotFoundException extends IllegalArgumentException
    {
        ResourceNotFoundException(Class<?> contextClass, String resourceName)
        {
            super(String.format("Could not find resource %s relative to class %s", resourceName, contextClass));
        }
    }

    private static Optional<URL> maybeGetResource(Class<?> contextClass, String resourceName)
    {
        return Optional.ofNullable(contextClass.getResource(resourceName));
    }

    public static URL getResource(Class<?> contextClass, String resourceName)
    {
        return maybeGetResource(contextClass, resourceName)
            .orElseThrow(() -> new ResourceNotFoundException(contextClass, resourceName));
    }

    private static Optional<InputStream> maybeGetResourceAsStream(Class<?> contextClass, String resourceName)
    {
        return Optional.ofNullable(contextClass.getResourceAsStream(resourceName));
    }

    public static InputStream getResourceAsStream(Class<?> contextClass, String resourceName)
    {
        return maybeGetResourceAsStream(contextClass, resourceName)
            .orElseThrow(() -> new ResourceNotFoundException(contextClass, resourceName));
    }

    public static Optional<String> maybeGetResourceAsString(Class<?> contextClass, String resourceName)
    {
        return maybeGetResourceAsStream(contextClass, resourceName)
            .map(stream -> Exceptions.getUncheckedIO(() -> {
                try (var ignored = stream)
                {
                    return new String(Exceptions.getUncheckedIO(stream::readAllBytes), StandardCharsets.UTF_8);
                }
            }));
    }

    public static String getResourceAsString(Class<?> contextClass, String resourceName)
    {
        return maybeGetResourceAsString(contextClass, resourceName)
            .orElseThrow(() -> new ResourceNotFoundException(contextClass, resourceName));
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
            for (var file : Objects.requireNonNull(resourceFile.listFiles()))
            {
                walkFileResourceTree(path + "/" + file.getName(), file, pathConsumer);
            }
        }
        else
        {
            pathConsumer.accept(path);
        }
    }

    public static void walkResourceTree(Class<?> contextClass, String path,
        BiConsumer<String, InputStream> pathAndContentConsumer)
    {
        final Consumer<String> pathConsumer = path_ -> Exceptions.runUncheckedIO(() -> {
            try (final var stream = getResourceAsStream(contextClass, path_))
            {
                pathAndContentConsumer.accept(path_, stream);
            }
        });

        final var resourceUrl = getResource(contextClass, path);
        if (resourceUrl.getProtocol().equals("jar"))
        {
            walkJarResourceTree(path, resourceUrl, pathConsumer);
        }
        else
        {
            walkFileResourceTree(path, new File(Exceptions.getUnchecked(resourceUrl::toURI)), pathConsumer);
        }
    }
}
