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
package com.datastax.fallout.test.utils;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;

import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ResourceUtils;

public abstract class WithTestResources extends WithTestNames
{
    public static String getTestClassResourceAsString(String path)
    {
        return getTestClassResourceAsString(currentTestClass(), path);
    }

    public static Path getTestClassResourceAsPath(String path)
    {
        return getTestClassResourceAsPath(currentTestClass(), path);
    }

    public static InputStream getTestClassResourceAsStream(String path)
    {
        return getTestClassResourceAsStream(currentTestClass(), path);
    }

    /** <code>path</code> can be relative or absolute; if it's absolute, we leave it
     *  alone, delegating the processing behaviour to {@link ResourceUtils} methods,
     *  which in turn will delegate the processing of <code>path</code> to the JDK. */
    private static String getClassBasedResourcePath(Class<?> testClass, String path)
    {
        if (path.startsWith("/"))
        {
            return path;
        }
        final ArrayList<String> enclosingClasses = new ArrayList<>();
        for (Class<?> enclosingClass = testClass;
            enclosingClass != null;
            enclosingClass = enclosingClass.getEnclosingClass())
        {
            enclosingClasses.add(enclosingClass.getSimpleName());
        }
        Collections.reverse(enclosingClasses);

        return String.join("/", enclosingClasses) + "/" + path;
    }

    /** Get a resource identified by path, from a resource directory identified by testClass.
     *
     * <p>This builds on the standard resource search by including the class names as well as the
     * package: for a class <code>foo.bar.Baz$Qux</code>, standard resource search will look in
     * <code>foo/bar</code>, whereas this method will look in <code>foo/bar/Baz/Qux</code>.  This is
     * useful for organising test resources by class instead of having them all at the package level.
     */
    public static String getTestClassResourceAsString(Class<?> testClass, String path)
    {
        final String classBasedResourcePath = getClassBasedResourcePath(testClass, path);
        return ResourceUtils.getResourceAsString(testClass, classBasedResourcePath);
    }

    /** Like {@link #getTestClassResourceAsString}, except it just returns a filesystem {@link Path} to the resource */
    public static Path getTestClassResourceAsPath(Class<?> testClass, String path)
    {
        final String classBasedResourcePath = getClassBasedResourcePath(testClass, path);
        return Path.of(Exceptions.getUnchecked(() -> ResourceUtils
            .getResource(testClass, classBasedResourcePath).toURI()));
    }

    /** Like {@link #getTestClassResourceAsString}, except it returns an {@link InputStream} for the resource */
    public static InputStream getTestClassResourceAsStream(Class<?> testClass, String path)
    {
        final String classBasedResourcePath = getClassBasedResourcePath(testClass, path);
        return ResourceUtils.getResourceAsStream(testClass, classBasedResourcePath);
    }
}
