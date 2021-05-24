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
package com.datastax.fallout.runner;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.util.JacksonUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.runner.ResourceLimitHelpers.limit;

public class ResourceLimitTest
{
    @Test
    public void deserializes_using_jackson() throws IOException
    {
        var yaml = String.join("\n",
            "- { provider: openstack, nodeLimit: 5 }",
            "- { provider: nebula, tenant: performance, nodeLimit: 6 }",
            "- { provider: ec2, tenant: sales, instanceType: c3.large, nodeLimit: 7 }");

        var mapper = JacksonUtils.getYamlObjectMapper();

        assertThat(mapper.readValue(yaml, new TypeReference<List<ResourceLimit>>() {}))
            .hasSameElementsAs(List.of(
                limit("openstack", 5),
                limit("nebula", "performance", 6),
                limit("ec2", "sales", "c3.large", 7)));
    }
}
