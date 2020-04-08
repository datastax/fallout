/*
 * Copyright 2020 DataStax, Inc.
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

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.BeforeClass;
import org.junit.runner.Description;

import com.datastax.fallout.util.Exceptions;

public abstract class WithPersistentTestOutputDir extends WithTestResources
{
    @BeforeClass
    public static void setTestBaseOutputDir()
    {
        TestOutputDir.setTestBaseOutputDir();
    }

    public static Path persistentTestOutputDir(Description description)
    {
        Path classOutputDir = TestOutputDir.getTestBaseOutputDir()
            .resolve(currentTestClassName(description));

        return Exceptions.getUncheckedIO(() -> Files.createDirectories(
            currentTestShortName(description)
                .map(classOutputDir::resolve)
                .orElse(classOutputDir)
                .toAbsolutePath()));
    }

    protected Path persistentTestOutputDir()
    {
        return Exceptions.getUncheckedIO(() -> Files.createDirectories(
            TestOutputDir.getTestBaseOutputDir()
                .resolve(currentTestClassName())
                .resolve(currentTestShortName())
                .toAbsolutePath()));
    }

    protected static Path persistentTestClassOutputDir()
    {
        return Exceptions.getUncheckedIO(() -> Files.createDirectories(
            TestOutputDir.getTestBaseOutputDir()
                .resolve(currentTestClassName())
                .resolve("class")
                .toAbsolutePath()));
    }
}
