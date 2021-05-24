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
package com.datastax.fallout.components.common.spec;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

public class YamlOperationsSpec
{
    private final PropertySpec<List<String>> deleteSpec;
    private final PropertySpec<Map<String, Object>> setSpec;
    private final PropertySpec<List<String>> getSpec;

    public YamlOperationsSpec(String prefix)
    {
        this(prefix, "");
    }

    public YamlOperationsSpec(String prefix, String subPrefix)
    {
        boolean hasSubPrefix = !subPrefix.isEmpty();
        String saneSubPrefix = subPrefix;
        if (hasSubPrefix && !subPrefix.endsWith("."))
        {
            saneSubPrefix += ".";
        }
        deleteSpec = PropertySpecBuilder.createStrList(prefix)
            .name(saneSubPrefix + "delete")
            .description("a list of Keys that are to be deleted in the specified yaml.\n" +
                "Keys must be provided in the form of 'parent.child.subchild'")
            .build();
        setSpec = PropertySpecBuilder
            .<Object>createMap(prefix)
            .name(saneSubPrefix + "set")
            .alias(prefix + subPrefix)
            .description("A Map of K V pairs that are to be set in the specified yaml.\n" +
                "Keys must be provided in the form of 'parent.child.subchild'")
            .build();
        getSpec = PropertySpecBuilder.createStrList(prefix)
            .name(saneSubPrefix + "get")
            .description("a list of Keys that are to be retrieved from the specified yaml.\n" +
                "Keys must be provided in the form of 'parent.child.subchild'")
            .build();
    }

    public Collection<PropertySpec<?>> getSpecs()
    {
        return List.of(deleteSpec, setSpec, getSpec);
    }

    public List<String> getDeleteOperations(PropertyGroup properties)
    {
        List<String> res = deleteSpec.value(properties);
        return res == null ? List.of() : res;
    }

    public Map<String, Object> getSetOperations(PropertyGroup properties)
    {
        Map<String, Object> res = setSpec.value(properties);
        return res == null ? Map.of() : res;
    }

    public List<String> getGetOperations(PropertyGroup properties)
    {
        List<String> res = getSpec.value(properties);
        return res == null ? List.of() : res;
    }
}
