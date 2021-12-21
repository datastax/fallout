/*
 * Copyright 2022 DataStax, Inc.
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

import java.util.UUID;

import org.junit.jupiter.api.Test;

import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.util.JsonUtils;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class TestRunStatusUpdateTest
{
    @Test
    void can_deserialize_json_from_earlier_versions()
    {
        // Extracted from a running fallout-1.266.0 RUNNER instance
        final var json =
            "{\"testRunIdentifier\":{\"testOwner\":\"owner@example.com\",\"testName\":\"http-docsapi-cql-keyvalue-casstrunk\",\"testRunId\":\"4ec86744-d72b-47da-9691-4ad66fa3e4a6\"},\"state\":\"RUNNING\"}";

        assertThat(JsonUtils.fromJson(json, TestRunStatusUpdate.class))
            .isEqualTo(new TestRunStatusUpdate(
                new TestRunIdentifier(
                    "owner@example.com",
                    "http-docsapi-cql-keyvalue-casstrunk",
                    UUID.fromString("4ec86744-d72b-47da-9691-4ad66fa3e4a6")),
                TestRun.State.RUNNING));
    }
}
