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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
}
