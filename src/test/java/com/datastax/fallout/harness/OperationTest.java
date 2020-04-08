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
package com.datastax.fallout.harness;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.MediaType;
import org.junit.Test;

import com.datastax.fallout.harness.impl.FakeModule;

import static org.assertj.core.api.Assertions.assertThat;

public class OperationTest
{
    private static ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    private static String toJson(Operation operation) throws JsonProcessingException
    {
        return jsonMapper.writeValueAsString(operation);
    }

    private static Map<String, Object> fromJson(String json) throws IOException
    {
        return jsonMapper.readValue(json, new TypeReference<Map<String, Object>>()
        {
        });
    }

    private static Module module = new FakeModule();

    private Operation operation(String process, Module module, long time, Operation.Type type)
    {
        return operation(process, module, time, type, null, null);
    }

    private Operation operation(String process, Module module, long time, Operation.Type type,
        MediaType mediaType, Object value)
    {
        return new Operation(process, module, time, type, mediaType, value);
    }

    @Test
    public void json_serialization_omits_null_mediatype_and_value() throws IOException
    {
        String json = toJson(operation("womble", module, 12345, Operation.Type.info));
        assertThat(fromJson(json)).doesNotContainKeys("mediaType", "value");
    }

    @Test
    public void json_serialization_includes_non_null_mediatype() throws IOException
    {
        String json = toJson(operation("womble", module, 12345, Operation.Type.info,
            MediaType.OCTET_STREAM, "foobar"));
        assertThat(fromJson(json)).containsKeys("mediaType", "value");
    }

}
