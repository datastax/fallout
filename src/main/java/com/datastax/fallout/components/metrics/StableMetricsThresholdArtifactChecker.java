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
package com.datastax.fallout.components.metrics;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import com.google.auto.service.AutoService;

import com.datastax.fallout.cassandra.shaded.com.google.common.annotations.VisibleForTesting;
import com.datastax.fallout.components.metrics.json.RangeQueryResult;
import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.JsonUtils;

import static com.datastax.fallout.components.metrics.json.RangeQueryResult.Metric;
import static com.datastax.fallout.components.metrics.json.RangeQueryResult.Result;
import static com.datastax.fallout.components.metrics.json.RangeQueryResult.Value;

@AutoService(ArtifactChecker.class)
public class StableMetricsThresholdArtifactChecker extends ArtifactChecker
{

    private static final String PREFIX = "fallout.artifact_checkers.metrics.";

    private static final PropertySpec<String> metricName = PropertySpecBuilder.createStr(PREFIX)
        .name("metric_name")
        .description(
            "The name of metric that should be analyzed. Please note that it should be a fully qualified metric name." +
                "Also, the name of the metrics will be used as the name of the file in the ARTIFACTS_DIRECTORY. I.e., if you set this to metric_a, the file name will be metric_a.json."
        )
        .required()
        .build();

    private static final PropertySpec<Duration> warmupOffset = PropertySpecBuilder.createDuration(PREFIX)
        .name("warmup_offset")
        .description(
            "If you want this artifact checker not to analyze metrics produced during your test's warmup phase, " +
                "you may offset the start of analysis by using this parameter. " +
                "The metric values before the threshold will be ignored.")
        .defaultOf(Duration.seconds(0))
        .build();

    private static final PropertySpec<Long> upperThreshold = PropertySpecBuilder.createLong(PREFIX)
        .name("upper_threshold")
        .description("The artifacts checker will validate if all metric value for the test run time " +
            "(plus optionally warmupOffset) was lower than the upper_threshold. If it is, the check will pass. " +
            "You may also set the lower_threshold for validating the lower bound of all metric values. " +
            "You need to set one or another, or both.")
        .build();

    private static final PropertySpec<Long> lowerThreshold = PropertySpecBuilder.createLong(PREFIX)
        .name("lower_threshold")
        .description("The artifacts checker will validate if all metric value for the test run time " +
            "(plus optionally warmupOffset) was higher than the lower_threshold. If it is, the check will pass. " +
            "You may also set the upper_threshold for validating the higher bound of all metric values. " +
            "You need to set one or another, or both.")
        .build();

    public static Path getMetricsArtifactsPath(NodeGroup nodeGroup)
    {
        return nodeGroup.getLocalArtifactPath().resolve("metrics");
    }

    @Override
    public boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        try
        {
            Path metricsRootPath = ensemble.getUniqueNodeGroupInstances().stream()
                .map(StableMetricsThresholdArtifactChecker::getMetricsArtifactsPath)
                .filter(Files::exists)
                .findFirst()
                .get();
            Path specificMetricPath = metricsRootPath.resolve(metricName.value(getProperties()) + ".json");
            RangeQueryResult rangeQueryResult = loadMetricValuesFromPath(specificMetricPath);
            logger().info("Validating metric file in {}", specificMetricPath);

            return validateIfMetricValuesAreWithinRange(rangeQueryResult,
                warmupOffset.value(getProperties()),
                lowerThreshold.value(getProperties()),
                upperThreshold.value(getProperties()));

        }
        catch (Exception exception)
        {
            logger().error("Problem when checking artifacts from: " + rootArtifactLocation, exception);
            return false;
        }
    }

    @VisibleForTesting
    boolean validateIfMetricValuesAreWithinRange(
        RangeQueryResult rangeQueryResult,
        Duration warmupOffset,
        Long lowerThreshold,
        Long upperThreshold)
    {
        for (Result result : rangeQueryResult.getData().getResult())
        {
            // this is executed per-node
            Metric metric = result.getMetric();
            Instant metricStartTime = null;

            for (Value value : result.getValues())
            {

                // the first value denotes the first produced metric for the given result;
                // if it is not present, take the first timestamp and use it as the first one.
                if (metricStartTime == null)
                {
                    metricStartTime = value.getTimestamp();
                }
                if (metricIsAfterWarmup(metricStartTime, warmupOffset, value.getTimestamp()) &&
                    (value.getValue() < lowerThreshold || value.getValue() > upperThreshold))
                {
                    logger().error("The value: {} for metric: {} is not within threshold ({}-{})",
                        value.getValue(),
                        metric,
                        lowerThreshold,
                        upperThreshold);
                    return false;
                }
            }
        }
        return true;
    }

    private boolean metricIsAfterWarmup(Instant metricStartTime, Duration warmupOffset, Instant currentValueTime)
    {
        Instant startTimePlusOffset = metricStartTime.plusSeconds(warmupOffset.toSeconds());
        return currentValueTime.isAfter(startTimePlusOffset) || currentValueTime.equals(startTimePlusOffset);
    }

    @VisibleForTesting
    RangeQueryResult loadMetricValuesFromPath(Path path)
    {
        String json = FileUtils.readString(path);
        return JsonUtils.fromJson(json, RangeQueryResult.class);
    }

    @Override
    public String prefix()
    {
        return PREFIX;
    }

    @Override
    public String name()
    {
        return "stable_metrics_threshold";
    }

    @Override
    public String description()
    {
        return "Checks if the value for a specific metricName is within lower_threshold-upper_threshold range. " +
            "If it is, the checks pass. If any of the processed value does not match this range, the check will fail. " +
            "You can filter out the metrics produced during warmup phase of your processing by setting the warmupOffset " +
            "parameter.";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(metricName, warmupOffset,
            upperThreshold, lowerThreshold);
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        if (upperThreshold.value(properties) == null && lowerThreshold.value(properties) == null)
        {
            throw new PropertySpec.ValidationException(
                "The upper_threshold or lower_threshold setting value must be set. " +
                    "You can also set both of those settings.");
        }
    }
}
