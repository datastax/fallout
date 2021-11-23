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

import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeAll;

import com.datastax.fallout.util.FileUtils;

public abstract class WithPersistentTestOutputDir extends WithTestResources
{
    @BeforeAll
    public static void setTestBaseOutputDir()
    {
        TestOutputDir.setTestBaseOutputDir();
    }

    private static Path persistentTestOutputDir(String currentTestClassName,
        Optional<String> currentTestShortName)
    {
        return FileUtils.createDirs(
            TestOutputDir.getTestBaseOutputDir()
                .resolve(currentTestClassName)
                .resolve(currentTestShortName.orElse("class"))
                .toAbsolutePath().toAbsolutePath());
    }

    public static Path persistentTestOutputDir(Class<?> testClass, Optional<Pair<Method, String>> testMethod)
    {
        return persistentTestOutputDir(testClassName(testClass),
            testMethod.map(testMethodAndDisplayName -> WithTestNames.fileNameFromTestRunName(
                testMethodAndDisplayName.getLeft(), testMethodAndDisplayName.getRight())));
    }

    protected Path persistentTestOutputDir()
    {
        return persistentTestOutputDir(currentTestClassName(), Optional.of(currentTestShortName()));
    }

    protected static Path persistentTestClassOutputDir()
    {
        return persistentTestOutputDir(currentTestClassName(), Optional.empty());
    }

    /** Calls {@link WithTestResources#getTestClassResource} on (testClass, resourcePath), writes the
     *  result to a file in {@link #persistentTestOutputDir()} with the same name as resourcePath, and returns the full path written.
     *
     *  <p>This is useful for copying resource data into a specific place for use by a test that
     *  needs to access the resource data as a file; in particular, integration tests that don't
     *  or can't poke around in the test resource file hierarchy (like the docker tests). */
    public Path writeTestClassResourceToFile(Class<?> testClass, String resourcePath)
    {
        final var path = persistentTestOutputDir().resolve(resourcePath);
        FileUtils.writeString(path, getTestClassResource(testClass, resourcePath));
        return path;
    }

    /** Calls {@link #writeTestClassResourceToFile(Class, String)} with
     *  {@link WithTestNames#currentTestClass()} as the first argument. */
    protected Path writeTestClassResourceToFile(String resourcePath)
    {
        return writeTestClassResourceToFile(currentTestClass(), resourcePath);
    }
}
