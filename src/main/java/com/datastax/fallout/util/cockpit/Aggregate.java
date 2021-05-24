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
package com.datastax.fallout.util.cockpit;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;

@AutoValue
@AutoValue.CopyAnnotations
@JsonSerialize(as = Aggregate.class)
@JsonPropertyOrder({"component", "category", "name", "value", "node", "phase"})
public abstract class Aggregate<T>
{
    public abstract String getComponent();

    public abstract String getCategory();

    public abstract String getName();

    public abstract T getValue();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract String getNode();

    @Nullable
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public abstract String getPhase();

    public static <T> Aggregate<T> of(String component, String category, String name, T value)
    {
        return Aggregate.of(component, category, name, value, null, null);
    }

    public static <T> Aggregate<T> of(String component, String category, String name, T value, String node)
    {
        return Aggregate.of(component, category, name, value, node, null);
    }

    @JsonCreator
    public static <T> Aggregate<T> of(String component, String category, String name, T value, String node,
        String phase)
    {
        return new AutoValue_Aggregate<T>(component, category, name, value, node, phase);
    }

    public Aggregate<T> withPhase(String phase)
    {
        return Aggregate.of(getComponent(), getCategory(), getName(), getValue(), getNode(), phase);
    }
}
