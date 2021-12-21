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
package com.datastax.fallout.service.views;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.ResourceType;
import com.datastax.fallout.runner.ResourceLimit;

/** Summarises the {@link ResourceLimit}s in effect against the in-use
 *  and requested {@link com.datastax.fallout.ops.ResourceRequirement}s */
public class ResourceUsageSummary
{
    private ResourceUsageSummary()
    {
        // utility class
    }

    private static String formatIntegerWithZeroAsBlank(int i)
    {
        return i > 0 ? String.valueOf(i) : "";
    }

    public interface ResourceUsageDisplay
    {
        @SuppressWarnings("unused")
        default String getInUseWithZeroAsBlank()
        {
            return formatIntegerWithZeroAsBlank(inUse());
        }

        @SuppressWarnings("unused")
        default String getRequestedWithZeroAsBlank()
        {
            return formatIntegerWithZeroAsBlank(requested());
        }

        int inUse();

        int requested();
    }

    public record ResourceLimitUsage(ResourceLimit limit, int inUse, int requested)
        implements
            ResourceUsageDisplay {
    }

    /** Combines the in-use and requested counts for a resource type in a single entry */
    public record ResourceUsage(ResourceType type, int inUse,
        int requested) implements Comparable<ResourceUsage>, ResourceUsageDisplay {
        public static ResourceUsage inUse(ResourceType type, int inUse)
        {
            return new ResourceUsage(type, inUse, 0);
        }

        public static ResourceUsage requested(ResourceType type, int requested)
        {
            return new ResourceUsage(type, 0, requested);
        }

        /** Combine this with other by summing the {@link #inUse} and {@link #requested()} values,
         * and return a new ResourceUsage. {@link #type()} must be the same in both this and other.  */
        public ResourceUsage merge(ResourceUsage other)
        {
            Preconditions.checkArgument(other.type().equals(type()));
            return new ResourceUsage(
                type(),
                inUse() + other.inUse(),
                requested() + other.requested());
        }

        @Override
        public int compareTo(ResourceUsage other)
        {
            return type().compareTo(other.type());
        }

        @SuppressWarnings("unused")
        public String getInUseWithZeroAsBlank()
        {
            return formatIntegerWithZeroAsBlank(inUse());
        }

        @SuppressWarnings("unused")
        public String getRequestedWithZeroAsBlank()
        {
            return formatIntegerWithZeroAsBlank(requested());
        }
    }

    private static List<ResourceUsage> mergeResourceUsages(Stream<ResourceUsage> usages)
    {
        return usages
            .collect(Collectors.groupingBy(
                ResourceUsage::type, Collectors.reducing(ResourceUsage::merge)))
            .values().stream()
            .flatMap(Optional::stream)
            .sorted()
            .toList();
    }

    /** Summarize resource usage by limits (if any).
     *
     * <p>As mentioned in {@link com.datastax.fallout.runner.ResourceLimiter}'s documentation, limits are
     * processed in order of definition, so this code groups in-use and requested resources in order of limits.
     */
    public static List<Pair<Optional<ResourceLimitUsage>, List<ResourceUsage>>> summarizeResourceUsage(
        List<ResourceLimit> resourceLimits,
        List<ResourceRequirement> activeResources,
        List<ResourceRequirement> requestedResources)
    {
        final var resourcesInUse = activeResources.stream()
            .map(req -> ResourceUsage.inUse(req.getResourceType(), req.getNodeCount()))
            .collect(Collectors.toCollection(ArrayList::new));

        final var resourcesRequested = requestedResources.stream()
            .map(req -> ResourceUsage.requested(req.getResourceType(), req.getNodeCount()))
            .collect(Collectors.toCollection(ArrayList::new));

        var resourceUsageGroupedByLimits = new ArrayList<Pair<Optional<ResourceLimitUsage>, List<ResourceUsage>>>();

        // Process limits in order of definition and remove matching usages
        for (var limit : resourceLimits)
        {
            final var resourceUsagesMatchingLimit = mergeResourceUsages(Stream.concat(
                resourcesInUse.stream().filter(usage -> limit.matchesInUse(usage.type())),
                resourcesRequested.stream().filter(usage -> limit.matchesRequested(usage.type()))));

            resourcesInUse.removeIf(usage -> limit.matchesInUse(usage.type()));
            resourcesRequested.removeIf(usage -> limit.matchesRequested(usage.type()));

            resourceUsageGroupedByLimits.add(Pair.of(
                Optional.of(new ResourceLimitUsage(
                    limit,
                    resourceUsagesMatchingLimit.stream().mapToInt(ResourceUsage::inUse).sum(),
                    resourceUsagesMatchingLimit.stream().mapToInt(ResourceUsage::requested).sum())),
                resourceUsagesMatchingLimit));
        }

        resourceUsageGroupedByLimits.add(Pair.of(Optional.empty(),
            mergeResourceUsages(Stream.concat(
                resourcesInUse.stream(),
                resourcesRequested.stream()))));

        return resourceUsageGroupedByLimits;
    }
}
