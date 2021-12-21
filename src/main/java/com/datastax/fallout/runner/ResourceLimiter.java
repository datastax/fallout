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

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.ResourceType;
import com.datastax.fallout.service.core.ReadOnlyTestRun;

/** Compares a set of {@link ResourceLimit} against the required and in-use resources, and
 * returns whether or not the limits will be exceeded by the testrun in the predicate.
 *
 * <p>A limit can be on:
 *
 * <ul>
     <li>a provider;
     <li>a provider and tenant;
     <li>a provider, tenant and instanceType.
 * </ul>
 *
 * When matching against required resources, resources with {@link ResourceType#getUniqueName} set are ignored.  The
 * rationale for this is that it's not currently possible to <em>quickly</em> check whether a named resource already
 * exists (we can see if it's in-use, but that's not the same as existing), so instead we assume it exists, and that it
 * isn't subject to limiting.  In-use named resources on the other hand <em>do</em> contribute towards in-use counts.
 *
 * <p>Limits are processed in order of definition, in case multiple limits match (which is allowed).
 * For example, if we want to limit all some-cloud:some-project usage to 80 instances, but also limit the
 * number of x3.huge instances on that tenant to 5, we can have a list of resourceLimits like this:
 *
 * <pre>
 * List.of(
 *     ResourceLimit.of(new ResourceType("some-cloud", "some-project", "x3.huge", Optional.empty()), 5),
 *     ResourceLimit.of(new ResourceType("some-cloud", "some-project", null, Optional.empty()), 80));
 * </pre>
 *
 * This isn't perfect: if a requested resource matches the first limit, even if 80
 * some-cloud:some-project instances are already in-use, the request will be marked available.  This
 * could be made more sophisticated, but we don't need that level of sophistication at the moment.
 */
public class ResourceLimiter implements Predicate<ReadOnlyTestRun>
{
    private static final Logger logger = LoggerFactory.getLogger(ResourceLimiter.class);

    private final Supplier<List<ResourceRequirement>> getResourcesInUse;
    private final List<ResourceLimit> limits;

    public ResourceLimiter(
        Supplier<List<ResourceRequirement>> getResourcesInUse,
        List<ResourceLimit> limits)
    {
        this.getResourcesInUse = getResourcesInUse;
        this.limits = limits;
    }

    private Optional<ResourceLimit> firstLimitMatchingRequired(ResourceType required)
    {
        return limits.stream()
            .filter(limit -> limit.matchesRequested(required))
            .findFirst();
    }

    private int allInUseMatching(ResourceLimit limit, List<ResourceRequirement> resourcesInUse)
    {
        return resourcesInUse.stream()
            .filter(inUse -> limit.matchesInUse(inUse.getResourceType()))
            .mapToInt(ResourceRequirement::getNodeCount)
            .sum();
    }

    private boolean available(ReadOnlyTestRun testRun, ResourceRequirement required)
    {
        final var resourcesInUse = getResourcesInUse.get();
        return firstLimitMatchingRequired(required.getResourceType())
            .map(limit -> {
                final var inUse = allInUseMatching(limit, resourcesInUse);
                boolean available = required.getNodeCount() + inUse <= limit.nodeLimit();
                if (!available)
                {
                    logger.info("TestRun {} {} {} not available for running because " +
                        "required ({}) + in-use ({}) exceeds resource limit ({})",
                        testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
                        required.getNodeCount(), inUse, limit);
                }
                return available;
            })
            .orElse(true);
    }

    @Override
    public boolean test(ReadOnlyTestRun testRun)
    {
        return testRun.getResourceRequirements().stream().allMatch(required_ -> available(testRun, required_));
    }
}
