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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonIgnoreProperties(ignoreUnknown = true)
public record RangeQueryResult(Data data) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Data(List<Result> result) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Metric(@JsonProperty("__name__") String name, String instance, String job) {
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Result(Metric metric, List<Value> values) {
    }

    @JsonDeserialize(using = CustomValueDeserializer.class)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Value(Instant timestamp, long value) {
    }

}
