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
package com.datastax.fallout.service.core;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.ops.ResourceRequirementHelpers.req;

public class DeletedTestRunTest
{
    @Test
    public void test_runs_and_deleted_test_runs_are_equal()
    {
        TestRun testRun = new TestRun();
        testRun.setOwner("moonunit@example.com");
        testRun.setTestName("dweezil");
        testRun.setTestRunId(UUID.fromString("795D3AE1-0358-44A2-9E79-1358BF524B24"));
        testRun.setCreatedAt(Date.from(Instant.now()));
        testRun.setStartedAt(Date.from(Instant.now().plusSeconds(5)));
        testRun.setFinishedAt(Date.from(Instant.now().plusSeconds(10)));
        testRun.setState(TestRun.State.PASSED);
        testRun.setDefinition("nope");
        testRun.setResults("maybe");
        testRun.updateArtifacts(Map.of("monolith", 1L, "ark-of-the-covenant", 2L, "mcguffin", 3L));
        testRun.setParsedLogInfo("parsed log info");
        testRun.setResourceRequirements(
            ImmutableSet
                .of(req("foo", "bar", "instance", 1)));

        DeletedTestRun deletedTestRun = DeletedTestRun.fromTestRun(testRun);
        assertThat(deletedTestRun.getOwner()).isEqualTo(testRun.getOwner());
        assertThat(deletedTestRun.getTestName()).isEqualTo(testRun.getTestName());
        assertThat(deletedTestRun.getTestRunId()).isEqualTo(testRun.getTestRunId());
        assertThat(deletedTestRun.getCreatedAt()).isEqualTo(testRun.getCreatedAt());
        assertThat(deletedTestRun.getStartedAt()).isEqualTo(testRun.getStartedAt());
        assertThat(deletedTestRun.getFinishedAt()).isEqualTo(testRun.getFinishedAt());
        assertThat(deletedTestRun.getState()).isEqualTo(testRun.getState());
        assertThat(deletedTestRun.getDefinition()).isEqualTo(testRun.getDefinition());
        assertThat(deletedTestRun.getResults()).isEqualTo(testRun.getResults());
        assertThat(deletedTestRun.getArtifacts()).isEqualTo(testRun.getArtifacts());
        assertThat(deletedTestRun.getParsedLogInfo()).isEqualTo(testRun.getParsedLogInfo());
        assertThat(deletedTestRun.getResourceRequirements()).isEqualTo(testRun.getResourceRequirements());
    }
}
