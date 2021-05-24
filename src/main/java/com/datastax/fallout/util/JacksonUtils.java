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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.dropwizard.jackson.Jackson;

public class JacksonUtils
{
    private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper()
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    private static final ObjectMapper YAML_OBJECT_MAPPER = Jackson.newObjectMapper(new YAMLFactory())
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public static ObjectMapper getObjectMapper()
    {
        return OBJECT_MAPPER;
    }

    public static ObjectMapper getYamlObjectMapper()
    {
        return YAML_OBJECT_MAPPER;
    }
}
