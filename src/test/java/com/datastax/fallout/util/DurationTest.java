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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class DurationTest
{
    public static Object[][] params()
    {
        return new Object[][] {
            {"0", Duration.seconds(0)},
            {"  0   ", Duration.seconds(0)},
            {"3061ns", Duration.nanoseconds(3061)},
            {"3061 nanoseconds", Duration.nanoseconds(3061)},
            {"401ms", Duration.milliseconds(401)},
            {"401 milliseconds ", Duration.milliseconds(401)},
            {"  5s", Duration.seconds(5)},
            {"5 s", Duration.seconds(5)},
            {"5 seconds", Duration.seconds(5)},
            {"10m", Duration.minutes(10)},
            {"  10 minutes  ", Duration.minutes(10)},
            {"7h", Duration.hours(7)},
            {"7 hours", Duration.hours(7)},
            {"12d", Duration.days(12)},
            {"12 days", Duration.days(12)}
        };
    }

    @ParameterizedTest(name = "fromString(\"{0}\") == {1}")
    @MethodSource("params")
    public void fromString_returns_expected_result(String input, Duration expected)
    {
        assertThat(Duration.fromString(input)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "fromString(\"{0}\") == {1}")
    @MethodSource("params")
    public void fromString_ignores_case(String input, Duration expected)
    {
        assertThat(Duration.fromString(input.toUpperCase())).isEqualTo(expected);
    }
}
