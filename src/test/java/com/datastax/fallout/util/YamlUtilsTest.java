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

import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.datastax.fallout.assertj.Assertions.assertThat;

class YamlUtilsTest
{
    private static List<Object[]> primitiveTypes()
    {
        return List.of(
            new Object[] {"42", 42},
            new Object[] {"42.3", 42.3},
            new Object[] {"true", true},
            new Object[] {"false", false},
            new Object[] {"TRUE", true},
            new Object[] {"FALSE", false},
            new Object[] {"\"FALSE\"", "FALSE"},
            new Object[] {"a random string", "a random string"},
            new Object[] {"-23", -23},
            new Object[] {"- 23", List.of(23)}
        );
    }

    @ParameterizedTest
    @MethodSource("primitiveTypes")
    public void primitive_types_can_be_loaded(String input, Object expected)
    {
        assertThat(YamlUtils.<Object>loadYamlWithType(input)).isEqualTo(expected);
    }
}
