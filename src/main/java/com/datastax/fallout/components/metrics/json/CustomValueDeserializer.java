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

import java.io.IOException;
import java.time.Instant;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import com.datastax.fallout.components.metrics.json.RangeQueryResult.Value;

public class CustomValueDeserializer extends StdDeserializer<Value>
{
    protected CustomValueDeserializer()
    {
        super(Value.class);
    }

    @Override
    public Value deserialize(JsonParser parser,
        DeserializationContext deserializationContext) throws IOException
    {
        ObjectCodec codec = parser.getCodec();
        JsonNode node = codec.readTree(parser);

        JsonNode timestamp = node.get(0);
        JsonNode metricValue = node.get(1);
        return Value.of(Instant.ofEpochSecond(timestamp.asLong()), Long.parseLong(metricValue.asText()));
    }
}
