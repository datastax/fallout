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
package com.datastax.fallout.harness.specs;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import com.datastax.fallout.ops.HasProperties;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.ops.PropertySpecBuilder.create;
import static com.datastax.fallout.ops.PropertySpecBuilder.createDuration;
import static com.datastax.fallout.ops.PropertySpecBuilder.createLong;

/**
 * Property specification for a timeout identified with a value and a unit.
 */
public class TimeoutSpec
{
    private final PropertySpec<Duration> timeout;
    private final PropertySpec<Long> timeoutValue;
    private final PropertySpec<TimeUnit> timeoutUnit;
    private final long defaultValue;
    private final TimeUnit defaultUnit;

    public TimeoutSpec(String prefix)
    {
        this(prefix, Duration.hours(1));
    }

    public TimeoutSpec(String prefix, Duration defaultDuration)
    {
        this(prefix, "timeout", defaultDuration);
    }

    public TimeoutSpec(String prefix, String name, Duration defaultDuration)
    {
        this(prefix, name, defaultDuration.value, defaultDuration.unit);
    }

    public TimeoutSpec(String prefix, String name, long defaultValue, TimeUnit defaultUnit)
    {
        this.defaultValue = defaultValue;
        this.defaultUnit = defaultUnit;
        timeout = createDuration(prefix).name(name)
            .description("The time allowed for the task to complete (default: " +
                new Duration(defaultValue, defaultUnit).toString() + ")")
            .build();
        timeoutValue = createLong(prefix).name(name + ".value")
            .description("The time allowed for the task to complete (deprecated, default: " + defaultValue + ")")
            .build();
        timeoutUnit = create(prefix).name(name + ".unit")
            .description("The time unit to use with the timeout value (deprecated, default: " + defaultUnit + ")")
            .options(TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES)
            .parser(s -> TimeUnit.valueOf(s.toString().toUpperCase()))
            .build();
    }

    public Collection<PropertySpec<?>> getSpecs()
    {
        return Arrays.asList(timeout, timeoutValue, timeoutUnit);
    }

    public Duration toDuration(HasProperties propertyObject)
    {
        return toDuration(propertyObject.getProperties());
    }

    public Duration toDuration(PropertyGroup propertyGroup)
    {
        Duration timeoutDuration = timeout.value(propertyGroup);
        if (timeoutDuration != null)
        {
            return timeoutDuration;
        }
        // deprecated properties
        Long timeValue = timeoutValue.value(propertyGroup);
        TimeUnit timeUnit = timeoutUnit.value(propertyGroup);
        if (timeValue == null && timeUnit != null)
        {
            timeValue = defaultValue;
        }
        if (timeValue != null && timeUnit == null)
        {
            timeUnit = defaultUnit;
        }
        if (timeValue != null && timeUnit != null)
        {
            return new Duration(timeValue, timeUnit);
        }
        // default
        return new Duration(defaultValue, defaultUnit);
    }
}
