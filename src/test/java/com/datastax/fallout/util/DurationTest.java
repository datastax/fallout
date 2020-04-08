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
package com.datastax.fallout.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
public class DurationTest
{
    @Parameter
    public String input;

    @Parameter(1)
    public Duration expected;

    @Parameters(name = "fromString(\"{0}\") == {1}")
    public static Object[] params()
    {
        return new Object[][] {
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

    @Test
    public void fromString_returns_expected_result()
    {
        assertThat(Duration.fromString(input)).isEqualTo(expected);
    }

    @Test
    public void fromString_ignores_case()
    {
        assertThat(Duration.fromString(input.toUpperCase())).isEqualTo(expected);
    }
}
