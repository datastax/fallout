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
package com.datastax.fallout.ops;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.ops.ResourceRequirementHelpers.req;

public class ResourceRequirementTest
{
    private List<TestRun> testRunsWithRequirements(ResourceRequirement... requirements)
    {
        ArrayList<TestRun> testRuns = new ArrayList<>();

        for (ResourceRequirement r : requirements)
        {
            TestRun testRun = new TestRun();
            HashSet<ResourceRequirement> reqs = new HashSet<>();
            reqs.add(r);
            testRun.setResourceRequirements(reqs);
            testRuns.add(testRun);
        }
        return testRuns;
    }

    private void assertGetResourcesRequirementsForTestRunsAre(List<TestRun> testRuns,
        ResourceRequirement... requirements)
    {
        assertThat(TestRun.getResourceRequirementsForTestRuns(testRuns)).containsExactlyInAnyOrder(requirements);
    }

    @Test
    public void getResourceRequirementsForTestRuns_aggregates_like_resources()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                req("foo", "bar", "instance1", 2),
                req("foo", "bar", "instance1", 1)
            ),
            req("foo", "bar", "instance1", 3)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_like_instances_across_providers_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                req("foo", "bar", "instance1", 2),
                req("provider2", "bar", "instance1", 2)
            ),
            req("foo", "bar", "instance1", 2),
            req("provider2", "bar", "instance1", 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_like_instances_across_tenant_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                req("foo", "bar", "instance1", 2),
                req("foo", "tenant2", "instance1", 2)
            ),
            req("foo", "bar", "instance1", 2),
            req("foo", "tenant2", "instance1", 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_different_instance_types_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                req("foo", "bar", "instance1", 2),
                req("foo", "bar", "instance2", 2)
            ),
            req("foo", "bar", "instance1", 2),
            req("foo", "bar", "instance2", 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_different_unique_names_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                req("foo", "bar", "instance1", "bravo", 2),
                req("foo", "bar", "instance1", "echo", 2)
            ),
            req("foo", "bar", "instance1", "bravo", 2),
            req("foo", "bar", "instance1", "echo", 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_treats_named_resources_as_unique()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                req("foo", "bar", "instance1", "bravo", 2),
                req("foo", "bar", "instance1", 2)
            ),
            req("foo", "bar", "instance1", "bravo", 2),
            req("foo", "bar", "instance1", 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRunsSortsCorrectly()
    {
        List<TestRun> testRuns = testRunsWithRequirements(
            req("b", "b", "instance", 2),
            req("b", "a", "instance", 2),
            req("c", "c", "instance", 2),
            req("a", "a", "instance", 2)
        );

        assertThat(TestRun.getResourceRequirementsForTestRuns(testRuns)).containsExactly(
            req("a", "a", "instance", 2),
            req("b", "a", "instance", 2),
            req("b", "b", "instance", 2),
            req("c", "c", "instance", 2)
        );
    }
}
