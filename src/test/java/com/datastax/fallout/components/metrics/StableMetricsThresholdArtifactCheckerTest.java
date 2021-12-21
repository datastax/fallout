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

import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.components.metrics.json.RangeQueryResult;
import com.datastax.fallout.components.metrics.json.RangeQueryResult.Data;
import com.datastax.fallout.components.metrics.json.RangeQueryResult.Metric;
import com.datastax.fallout.components.metrics.json.RangeQueryResult.Result;
import com.datastax.fallout.components.metrics.json.RangeQueryResult.Value;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.JsonUtils;
import com.datastax.fallout.util.ResourceUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;

class StableMetricsThresholdArtifactCheckerTest
{
    @Test
    public void shouldLoadMetricsFromFile()
    {
        String json = ResourceUtils.getResourceAsString(getClass(), "metric_a.json");
        RangeQueryResult rangeQueryResult = JsonUtils.fromJson(json, RangeQueryResult.class);

        List<Result> result = rangeQueryResult.data().result();
        assertThat(result).containsExactly(
            new Result(new Metric("metric_a", "123.123.123.134:8080", "some_job"),
                List.of(new Value(Instant.ofEpochSecond(1612169177), 0L), new Value(
                    Instant.ofEpochSecond(1612169192), 5L
                ))),
            new Result(new Metric("metric_a", "123.123.123.135:8084", "some_job"),
                List.of(new Value(Instant.ofEpochSecond(1612169177), 15L))));

    }

    public static Stream<Value> metricsWithinRange()
    {
        return Stream.of(
            new Value(Instant.ofEpochSecond(1612169170), 15L),
            new Value(Instant.ofEpochSecond(1612169170), 0L),
            new Value(Instant.ofEpochSecond(1612169170), 5L));
    }

    @ParameterizedTest
    @MethodSource("metricsWithinRange")
    public void shouldReturnTrueIfMetricsAreWithinRange(Value inRangeValue)
    {
        StableMetricsThresholdArtifactChecker stableMetricsThresholdArtifactChecker =
            new StableMetricsThresholdArtifactChecker();
        List<Result> metricResults =
            List.of(
                new Result(new Metric("metric_a", "123.123.123.134:8080", "some_job"),
                    List.of(new Value(Instant.ofEpochSecond(1612169192), 0L), inRangeValue)),
                new Result(new Metric("metric_a", "123.123.123.135:8084", "some_job"),
                    List.of(new Value(Instant.ofEpochSecond(1612169140), 15L))));

        RangeQueryResult rangeQueryResult = new RangeQueryResult(new Data(metricResults));

        boolean result = stableMetricsThresholdArtifactChecker
            .validateIfMetricValuesAreWithinRange(rangeQueryResult, Duration.seconds(0), 0L, 15L);
        assertThat(result).isTrue();
    }

    public static Stream<Value> metricValueOutOfRange()
    {
        return Stream.of(
            new Value(Instant.ofEpochSecond(1612169170), 16L),
            new Value(Instant.ofEpochSecond(1612169170), -1L));
    }

    @ParameterizedTest
    @MethodSource("metricValueOutOfRange")
    public void shouldReturnFalseIfMetricsAreNotWithinRange(Value outOfRangeValue)
    {
        StableMetricsThresholdArtifactChecker stableMetricsThresholdArtifactChecker =
            new StableMetricsThresholdArtifactChecker();
        List<Result> metricResults = List.of(new Result(new Metric("metric_a", "123.123.123.134:8080", "some_job"),
            List.of(
                new Value(Instant.ofEpochSecond(1612169140), 0L),
                new Value(Instant.ofEpochSecond(1612169170), 5L))),
            (new Result(new Metric("metric_a", "123.123.123.135:8084", "some_job"),
                List.of(outOfRangeValue))));

        RangeQueryResult rangeQueryResult = new RangeQueryResult(new Data(metricResults));

        boolean result = stableMetricsThresholdArtifactChecker
            .validateIfMetricValuesAreWithinRange(rangeQueryResult, Duration.seconds(0), 0L, 15L);
        assertThat(result).isFalse();
    }

    @Test
    public void shouldIgnoreOutOfRangeMetricIfItIsBeforeWarmupOffset()
    {
        StableMetricsThresholdArtifactChecker stableMetricsThresholdArtifactChecker =
            new StableMetricsThresholdArtifactChecker();
        Duration warmupOffset = Duration.seconds(60);
        Instant now = Instant.now();
        List<Result> metricResults = List.of(new Result(new Metric("metric_a", "123.123.123.134:8080", "some_job"),
            List.of(
                new Value(now, 100L),
                new Value(now.plusSeconds(60), 5L))),
            new Result(new Metric("metric_a", "123.123.123.135:8084", "some_job"),
                List.of(new Value(now, 15L))));

        RangeQueryResult rangeQueryResult = new RangeQueryResult(new Data(metricResults));

        boolean result = stableMetricsThresholdArtifactChecker
            .validateIfMetricValuesAreWithinRange(rangeQueryResult, warmupOffset, 0L, 15L);
        assertThat(result).isTrue();
    }

    @Test
    public void shouldCatchOutOfRangeMetricWhenUsingWarmupOffset()
    {
        StableMetricsThresholdArtifactChecker stableMetricsThresholdArtifactChecker =
            new StableMetricsThresholdArtifactChecker();
        Duration warmupOffset = Duration.seconds(60);
        Instant now = Instant.now();
        List<Result> metricResults = List.of(new Result(new Metric("metric_a", "123.123.123.134:8080", "some_job"),
            List.of(
                new Value(now, 100L),
                new Value(now.plusSeconds(60), 33L))),
            new Result(new Metric("metric_a", "123.123.123.135:8084", "some_job"),
                List.of(new Value(now, 100L))));

        RangeQueryResult rangeQueryResult = new RangeQueryResult(new Data(metricResults));

        boolean result = stableMetricsThresholdArtifactChecker
            .validateIfMetricValuesAreWithinRange(rangeQueryResult, warmupOffset, 0L, 15L);
        assertThat(result).isFalse();
    }
}
