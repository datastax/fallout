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
package com.datastax.fallout.components.metrics.json;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
@AutoValue.CopyAnnotations
@JsonSerialize(as = RangeQueryResult.class)
public abstract class RangeQueryResult
{
    @JsonCreator
    public static RangeQueryResult of(Data data)
    {
        return new AutoValue_RangeQueryResult(data);
    }

    public abstract Data getData();

    @AutoValue
    @JsonIgnoreProperties(ignoreUnknown = true)
    @AutoValue.CopyAnnotations
    @JsonSerialize(as = Data.class)
    public abstract static class Data
    {

        @JsonCreator
        public static Data of(List<Result> result)
        {
            return new AutoValue_RangeQueryResult_Data(result);
        }

        public abstract List<Result> getResult();

    }

    @AutoValue
    @JsonIgnoreProperties(ignoreUnknown = true)
    @AutoValue.CopyAnnotations
    @JsonSerialize(as = Metric.class)
    public abstract static class Metric
    {
        @JsonCreator
        public static Metric of(String name, String instance, String job)
        {
            return new AutoValue_RangeQueryResult_Metric(name, instance, job);
        }

        @JsonProperty("__name__")
        public abstract String getName();

        public abstract String getInstance();

        public abstract String getJob();

    }

    @AutoValue
    @JsonIgnoreProperties(ignoreUnknown = true)
    @AutoValue.CopyAnnotations
    @JsonSerialize(as = Result.class)
    public abstract static class Result
    {

        @JsonCreator
        public static Result of(Metric metric, List<Value> values)
        {
            return new AutoValue_RangeQueryResult_Result(metric, values);
        }

        public abstract Metric getMetric();

        public abstract List<Value> getValues();

    }

    @JsonDeserialize(using = CustomValueDeserializer.class)
    @AutoValue
    @JsonIgnoreProperties(ignoreUnknown = true)
    @AutoValue.CopyAnnotations
    @JsonSerialize(as = Value.class)
    public abstract static class Value
    {

        @JsonCreator
        public static Value of(Instant timestamp, long value)
        {
            return new AutoValue_RangeQueryResult_Value(timestamp, value);
        }

        public abstract Instant getTimestamp();

        public abstract long getValue();

    }

}
