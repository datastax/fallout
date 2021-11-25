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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils
{
    private static final ObjectMapper OBJECT_MAPPER = JacksonUtils.getObjectMapper();

    public static <T> T fromJson(String json, Class<T> clazz)
    {
        return Exceptions.getUncheckedIO(() -> OBJECT_MAPPER.readValue(json, clazz));
    }

    public static <T> T fromJsonAtPath(Path path, Class<T> clazz)
    {
        String jsonContent = FileUtils.readString(path);
        return fromJson(jsonContent, clazz);
    }

    public static <T> T fromJson(String json, TypeReference<T> typeReference)
    {
        return Exceptions.getUncheckedIO(() -> OBJECT_MAPPER.readValue(json, typeReference));
    }

    public static JsonNode getJsonNode(String json)
    {
        return Exceptions.getUncheckedIO(() -> OBJECT_MAPPER.readTree(json));
    }

    public static JsonNode getJsonNode(String json, String pathToNode)
    {
        return getJsonNode(json).at(pathToNode);
    }

    public static Map<String, String> fromJsonMap(String json)
    {
        return fromJson(json, new TypeReference<>() {});
    }

    public static List<String> fromJsonList(String json)
    {
        return fromJson(json, new TypeReference<>() {});
    }

    public static String toJson(Object object)
    {
        return Exceptions.getUncheckedIO(() -> OBJECT_MAPPER.writeValueAsString(object));
    }

    /** Evil hack: this creates a copy of <code>source</code> as a different class with identical
     * fields, <code>DestClass</code>.
     *
     * <p>The only valid usage for this technique is when SourceClass
     * and DestClass only differ in their name, and the only reason for that is that the C* driver
     * mapper (as of v3) requires each database table to have a unique type associated with it, so you
     * can't store (say) {@link com.datastax.fallout.service.core.TestRun} in two separate tables.  */
    public static <SourceClass, DestClass> DestClass copyUsingSerialization(
        SourceClass source, Class<DestClass> destClass)
    {
        return Exceptions.getUnchecked(() -> {
            byte[] json = OBJECT_MAPPER.writeValueAsBytes(source);
            return OBJECT_MAPPER.readValue(json, destClass);
        });
    }

    /**
     * Parses the only value from a JSON singleton map:
     *
     *  {
     *    "0": "/var/lib/cassandra/data"
     *  }
     *
     * Returns "/var/lib/cassandra/data"
     *
     *  {
     *    "0": [
     *      "/var/lib/cassandra/data",
     *      "/mnt/cass_data_disks/data1"
     *    ]
     *  }
     *
     * Returns ["/var/lib/cassandra/data", "/mnt/cass_data_disks/data1"]
     */
    public static <T> T fromJsonSingletonMap(String json)
    {
        Map<String, T> output = fromJson(json, new TypeReference<>() {});

        if (output.size() != 1)
        {
            throw new IllegalArgumentException(String.format(
                "fromJsonSingletonMap expects a map with a single entry but was given:\n%s", json));
        }

        return output.values().iterator().next();
    }
}
