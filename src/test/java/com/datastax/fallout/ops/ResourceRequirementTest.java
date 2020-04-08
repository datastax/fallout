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
package com.datastax.fallout.ops;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.junit.Test;

import com.datastax.fallout.service.core.TestRun;

import static org.assertj.core.api.Assertions.assertThat;

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
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2),
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 1)
            ),
            new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 3)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_like_instances_across_providers_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2),
                new ResourceRequirement(new ResourceRequirement.ResourceType("provider2", "bar", "instance1"), 2)
            ),
            new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("provider2", "bar", "instance1"), 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_like_instances_across_tenant_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2),
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "tenant2", "instance1"), 2)
            ),
            new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "tenant2", "instance1"), 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_different_instance_types_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2),
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance2"), 2)
            ),
            new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance2"), 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_keeps_different_unique_names_separate()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                new ResourceRequirement(
                    new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("bravo")), 2),
                new ResourceRequirement(
                    new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("echo")), 2)
            ),
            new ResourceRequirement(
                new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("bravo")), 2),
            new ResourceRequirement(
                new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("echo")), 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRuns_treats_named_resources_as_unique()
    {
        assertGetResourcesRequirementsForTestRunsAre(
            testRunsWithRequirements(
                new ResourceRequirement(
                    new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("bravo")), 2),
                new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2)
            ),
            new ResourceRequirement(
                new ResourceRequirement.ResourceType("foo", "bar", "instance1", Optional.of("bravo")), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("foo", "bar", "instance1"), 2)
        );
    }

    @Test
    public void getResourceRequirementsForTestRunsSortsCorrectly()
    {
        List<TestRun> testRuns = testRunsWithRequirements(
            new ResourceRequirement(new ResourceRequirement.ResourceType("b", "b", "instance"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("b", "a", "instance"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("c", "c", "instance"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("a", "a", "instance"), 2)
        );

        assertThat(TestRun.getResourceRequirementsForTestRuns(testRuns)).containsExactly(
            new ResourceRequirement(new ResourceRequirement.ResourceType("a", "a", "instance"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("b", "a", "instance"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("b", "b", "instance"), 2),
            new ResourceRequirement(new ResourceRequirement.ResourceType("c", "c", "instance"), 2)
        );
    }
}
