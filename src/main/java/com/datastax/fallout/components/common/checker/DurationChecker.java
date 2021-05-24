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
package com.datastax.fallout.components.common.checker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.util.Duration;

/**
 * A checker that validates duration of phases or the whole workload using timestamps in jepsen history.
 */

@AutoService(Checker.class)
public class DurationChecker extends Checker
{
    static final String prefix = "fallout.checkers.duration";

    static final PropertySpec<Duration> maxDurationSpec = PropertySpecBuilder.createDuration(prefix)
        .name("duration.max")
        .description("The max acceptable run time for the workload / phase.")
        .build();

    static final PropertySpec<Duration> minDurationSpec = PropertySpecBuilder.createDuration(prefix)
        .name("duration.min")
        .description("The minimum acceptable run time for the workload / phase.")
        .build();

    static final PropertySpec<List<String>> phaseListSpec = PropertySpecBuilder
        .<List<String>>create(prefix)
        .name("phases")
        .description("Comma-separated list of names of phases to measure. " +
            "The non-overlapping SUM of these phases' durations is checked against the min and max properties. " +
            "To check multiple phases without summing, use multiple instances of this checker. " +
            "If omitted, time of the entire workload is used (excluding setup & teardown). ")
        .parser(input -> List.of(((String) input).split(",")))
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "duration";
    }

    @Override
    public String description()
    {
        return "Checks that the test or a set of phases does not run for too long. Can also check for not running long enough. " +
            "If a list of phase names is not specified, the entire workload is timed (excludes setup and teardown). " +
            "This checker may be used multiple times with different properties.";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(maxDurationSpec)
            .add(minDurationSpec)
            .add(phaseListSpec)
            .build();
    }

    @Override
    public boolean checkHistory(Ensemble ensemble, Collection<Operation> history)
    {
        long duration = 0;
        Duration maxDuration = maxDurationSpec.value(this);
        Duration minDuration = minDurationSpec.value(this);
        List<String> phaseNames = phaseListSpec.value(this);
        phaseNames = (phaseNames == null) ? new ArrayList<>() : phaseNames;
        String description = "test";
        if (phaseNames.isEmpty())
        {
            long start = 0;
            long end = 0;
            for (Operation op : history)
            {
                if (start == 0)
                {
                    start = op.getTime();
                }
                end = op.getTime();
            }
            duration = end - start;
        }
        else
        {
            phaseNames.replaceAll(n -> n.trim());
            description = "phase(s) " + phaseNames.toString();

            // With multiple phases of interest, we have to keep track of context to avoid double-counting
            int contextCounter = 0;
            long start = 0;
            for (Operation op : history)
            {
                if (op.getType() == Operation.Type.start && phaseNames.contains(op.getProcess()))
                {
                    contextCounter++;
                    if (contextCounter == 1)
                    {
                        start = op.getTime();
                    }
                }
                else if (op.getType() == Operation.Type.end && phaseNames.contains(op.getProcess()))
                {
                    contextCounter--;
                    if (contextCounter == 0)
                    {
                        duration += op.getTime() - start;
                    }
                }
            }
            if (contextCounter > 0)
            {
                logger().warn(
                    "DurationChecker didn't find the end of a phase. Calculated duration will not include any time for such phases.");
            }

            if (duration == 0)
            {
                logger().warn(
                    "Total duration of {} is 0 - verify that the phase name(s) match the workload section of the test yaml.",
                    description);
            }
        }

        boolean durationOk = true;
        if (maxDuration != null && duration > maxDuration.toNanos())
        {
            durationOk = false;
            String convertedDuration = maxDuration.unit.convert(duration, TimeUnit.NANOSECONDS) + " " +
                maxDuration.unit.toString().toLowerCase();
            logger().warn("Duration of {} was {} ({} nanos), exceeding the max specified in checker: {}", description,
                convertedDuration, duration, maxDuration.toString());
        }
        else if (minDuration != null && duration < minDuration.toNanos())
        {
            durationOk = false;
            String convertedDuration = minDuration.unit.convert(duration, TimeUnit.NANOSECONDS) + " " +
                minDuration.unit.toString().toLowerCase();
            logger().warn("Duration of {} was {} ({} nanos), & was shorter than the min specified in checker: {}",
                description, convertedDuration, duration, minDuration.toString());
        }
        return durationOk;
    }
}
