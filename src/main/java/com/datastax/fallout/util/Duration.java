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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class Duration
{
    private static final Duration ZERO_DURATION = new Duration(0L, TimeUnit.SECONDS);
    private static final Pattern DURATION_RE =
        Pattern.compile(
            "^(\\d+)\\s*(ns|ms|s|m|h|d|" +
                Joiner.on("|").join(TimeUnit.values()) +
                ")$",
            Pattern.CASE_INSENSITIVE);

    public static Duration fromString(String value)
    {
        if (value == null)
            return null;

        String trimValue = value.trim();
        if (trimValue.equals("0"))
        {
            return ZERO_DURATION;
        }
        Matcher match = DURATION_RE.matcher(trimValue);
        if (!match.matches())
        {
            throw new IllegalArgumentException(value);
        }

        long duration = Long.parseLong(match.group(1));
        TimeUnit durationUnit;

        switch (match.group(2).toLowerCase())
        {
            case "ns":
                durationUnit = TimeUnit.NANOSECONDS;
                break;
            case "ms":
                durationUnit = TimeUnit.MILLISECONDS;
                break;
            case "s":
                durationUnit = TimeUnit.SECONDS;
                break;
            case "m":
                durationUnit = TimeUnit.MINUTES;
                break;
            case "h":
                durationUnit = TimeUnit.HOURS;
                break;
            case "d":
                durationUnit = TimeUnit.DAYS;
                break;
            default:
                durationUnit = TimeUnit.valueOf(match.group(2).toUpperCase());
                break;
        }

        return new Duration(duration, durationUnit);
    }

    public static Duration nanoseconds(long nanos)
    {
        return new Duration(nanos, TimeUnit.NANOSECONDS);
    }

    public static Duration milliseconds(long millis)
    {
        return new Duration(millis, TimeUnit.MILLISECONDS);
    }

    public static Duration seconds(long seconds)
    {
        return new Duration(seconds, TimeUnit.SECONDS);
    }

    public static Duration minutes(long value)
    {
        return new Duration(value, TimeUnit.MINUTES);
    }

    public static Duration hours(long hours)
    {
        return new Duration(hours, TimeUnit.HOURS);
    }

    public static Duration days(long days)
    {
        return new Duration(days, TimeUnit.DAYS);
    }

    public static Duration fromJdkDuration(java.time.Duration duration)
    {
        return new Duration(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    public final Long value;
    public final TimeUnit unit;

    public Duration(Long value, TimeUnit unit)
    {
        Preconditions.checkNotNull(value);
        Preconditions.checkArgument(value >= 0);
        this.value = value;
        this.unit = unit;
    }

    public long toNanos()
    {
        return unit.toNanos(value);
    }

    public long toMillis()
    {
        return unit.toMillis(value);
    }

    public long toSeconds()
    {
        return unit.toSeconds(value);
    }

    public long toMinutes()
    {
        return unit.toMinutes(value);
    }

    public long toHours()
    {
        return unit.toHours(value);
    }

    @Override
    public String toString()
    {
        return value + " " + unit.toString().toLowerCase();
    }

    public String toAbbrevString()
    {
        String abbrev = null;
        switch (this.unit)
        {
            case NANOSECONDS:
                abbrev = "ns";
                break;
            case MILLISECONDS:
                abbrev = "ms";
                break;
            case SECONDS:
                abbrev = "s";
                break;
            case MINUTES:
                abbrev = "m";
                break;
            case HOURS:
                abbrev = "h";
                break;
            case DAYS:
                abbrev = "d";
                break;
        }
        return String.format("%s%s", this.value, abbrev);
    }

    public String toHM()
    {
        String hrs = "";
        long min = toMinutes();
        if (min >= 60)
        {
            hrs += min / 60 + " hr";
            min = min % 60;
            if (min == 0)
            {
                return hrs;
            }
            else
            {
                hrs += ", ";
            }
        }
        return hrs + min + " min";
    }

    public String toHMS()
    {
        long sec = toSeconds();
        if (sec >= 60)
        {
            return toHM() + ", " + sec % 60 + " sec";
        }
        return sec + " sec";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        return ((Duration) o).toNanos() == toNanos();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(toNanos());
    }
}
