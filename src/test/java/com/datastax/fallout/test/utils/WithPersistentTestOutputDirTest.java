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

import java.io.IOException;
import java.nio.file.Files;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class WithPersistentTestOutputDirTest extends WithPersistentTestOutputDir
{
    @Test
    public void current_test_class_name_is_the_class()
    {
        assertThat(currentTestClassName()).isEqualTo("WithPersistentTestOutputDirTest");
    }

    @Test
    public void current_test_short_name_is_the_method()
    {
        assertThat(currentTestShortName()).isEqualTo("current_test_short_name_is_the_method");
    }

    @Test
    public void persistent_test_output_dir_contains_the_class_and_method()
    {
        assertThat(persistentTestOutputDir().toString())
            .endsWith("/WithPersistentTestOutputDirTest/persistent_test_output_dir_contains_the_class_and_method");
    }

    @Test
    public void persistent_test_class_output_dir_contains_the_class()
    {
        assertThat(persistentTestClassOutputDir().toString())
            .endsWith("/WithPersistentTestOutputDirTest/class");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void parameterized_tests_get_unique_output_dirs(int value) throws IOException
    {
        final var nonExistentFile = persistentTestOutputDir().resolve("foo");
        assertThat(nonExistentFile).doesNotExist();
        Files.writeString(nonExistentFile, "This file should not already exist");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void parameterized_tests_include_the_method_name(int value)
    {
        assertThat(currentTestShortName()).startsWith("parameterized_tests_include_the_method_name");
    }

    @ParameterizedTest(name = "[{index}] {0} This parameterized test name is far too long")
    @ValueSource(ints = {1, 2})
    public void parameterized_tests_with_long_display_names_are_truncated(int value)
    {
        assertThat(currentTestShortName())
            .matches(String.format("parameterized_tests_with_long_display_names_are_truncated" +
                "\\[\\[%d\\]-%d-This-paramete...[0-9a-f]{8}\\]",
                value, value));
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"example.yaml", "foo/bar/baz.yaml"})
    public void parameterized_tests_with_yaml_file_names_use_the_file_name_as_the_test_name(String fileName)
    {
        assertThat(currentTestShortName())
            .isEqualTo(fileName.replaceAll("/", "-"));
    }
}
