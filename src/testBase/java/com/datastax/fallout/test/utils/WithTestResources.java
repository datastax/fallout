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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;

import com.google.common.io.Resources;

import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ResourceUtils;

public abstract class WithTestResources extends WithTestNames
{
    public static String getTestClassResource(String path)
    {
        return getTestClassResource(currentTestClass(), path);
    }

    public static Path getTestClassResourcePath(String path)
    {
        return getTestClassResourcePath(currentTestClass(), path);
    }

    private static String getClassBasedResourcePath(Class<?> testClass, String path)
    {
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
    public static String getTestClassResource(Class<?> testClass, String path)
    {
        final String classBasedResourcePath = getClassBasedResourcePath(testClass, path);
        return ResourceUtils.readResourceAsString(testClass, classBasedResourcePath);
    }

    /** Like {@link #getTestClassResource}, except it just returns a filesystem {@link Path} to the resource */
    public static Path getTestClassResourcePath(Class<?> testClass, String path)
    {
        final String classBasedResourcePath = getClassBasedResourcePath(testClass, path);
        return Path.of(Exceptions.getUnchecked(() -> Resources.getResource(testClass, classBasedResourcePath).toURI()));
    }
}
