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
}
