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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
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
            return formatIntegerWithZeroAsBlank(getInUse());
        }

        @SuppressWarnings("unused")
        default String getRequestedWithZeroAsBlank()
        {
            return formatIntegerWithZeroAsBlank(getRequested());
        }

        int getInUse();

        int getRequested();
    }

    @AutoValue
    public static abstract class ResourceLimitUsage implements ResourceUsageDisplay
    {
        public static ResourceLimitUsage of(ResourceLimit limit, int inUse, int requested)
        {
            return new AutoValue_ResourceUsageSummary_ResourceLimitUsage(inUse, requested, limit);
        }

        public abstract ResourceLimit getLimit();
    }

    /** Combines the in-use and requested counts for a resource type in a single entry */
    @AutoValue
    public static abstract class ResourceUsage implements Comparable<ResourceUsage>, ResourceUsageDisplay
    {
        public static ResourceUsage inUse(ResourceType type, int inUse)
        {
            return of(type, inUse, 0);
        }

        public static ResourceUsage requested(ResourceType type, int requested)
        {
            return of(type, 0, requested);
        }

        @VisibleForTesting
        public static ResourceUsage of(ResourceType type, int inUse, int requested)
        {
            return new AutoValue_ResourceUsageSummary_ResourceUsage(inUse, requested, type);
        }

        /** Combine this with other by summing the {@link #getInUse} and {@link #getRequested()} values,
         * and return a new ResourceUsage. {@link #getType()} must be the same in both this and other.  */
        public ResourceUsage merge(ResourceUsage other)
        {
            Preconditions.checkArgument(other.getType().equals(getType()));
            return new AutoValue_ResourceUsageSummary_ResourceUsage(
                getInUse() + other.getInUse(),
                getRequested() + other.getRequested(),
                getType());
        }

        @Override
        public int compareTo(ResourceUsage other)
        {
            return getType().compareTo(other.getType());
        }

        @SuppressWarnings("unused")
        public String getInUseWithZeroAsBlank()
        {
            return formatIntegerWithZeroAsBlank(getInUse());
        }

        @SuppressWarnings("unused")
        public String getRequestedWithZeroAsBlank()
        {
            return formatIntegerWithZeroAsBlank(getRequested());
        }

        public abstract ResourceType getType();
    }

    private static List<ResourceUsage> mergeResourceUsages(Stream<ResourceUsage> usages)
    {
        return usages
            .collect(Collectors.groupingBy(
                ResourceUsage::getType, Collectors.reducing(ResourceUsage::merge)))
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
                resourcesInUse.stream().filter(usage -> limit.matchesInUse(usage.getType())),
                resourcesRequested.stream().filter(usage -> limit.matchesRequested(usage.getType()))));

            resourcesInUse.removeIf(usage -> limit.matchesInUse(usage.getType()));
            resourcesRequested.removeIf(usage -> limit.matchesRequested(usage.getType()));

            resourceUsageGroupedByLimits.add(Pair.of(
                Optional.of(ResourceLimitUsage.of(
                    limit,
                    resourceUsagesMatchingLimit.stream().mapToInt(ResourceUsage::getInUse).sum(),
                    resourceUsagesMatchingLimit.stream().mapToInt(ResourceUsage::getRequested).sum())),
                resourceUsagesMatchingLimit));
        }

        resourceUsageGroupedByLimits.add(Pair.of(Optional.empty(),
            mergeResourceUsages(Stream.concat(
                resourcesInUse.stream(),
                resourcesRequested.stream()))));

        return resourceUsageGroupedByLimits;
    }
}
