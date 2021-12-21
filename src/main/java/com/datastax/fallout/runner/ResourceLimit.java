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

import javax.annotation.Nullable;

import java.util.Optional;

import com.datastax.fallout.ops.ResourceType;

/** Represents a resource limit; see {@link #matchesInUse} for matching rules */
public record ResourceLimit(
    String provider,
    @Nullable String tenant,
    @Nullable String instanceType,
    int nodeLimit) {
    public static ResourceLimit of(String provider, String tenant, String instanceType, int nodeLimit)
    {
        return new ResourceLimit(provider, tenant, instanceType, nodeLimit);
    }

    private boolean matches(ResourceType target)
    {
        final var resourceType =
            new ResourceType(this.provider(), this.tenant(), this.instanceType(), Optional.empty());

        return resourceType.equals(target.copyOnlyProvider()) ||
            resourceType.equals(target.copyOnlyProviderAndTenant()) ||
            resourceType.equals(target.copyWithoutUniqueName());
    }

    /** Matches the {@link ResourceType} in this limit against <code>target</code> as follows:
     *  <p>
     *  If the limit specifies:
     *  <ul>
     *      <li>only a provider, then only the provider in <code>target</code>
     *      is matched against;
     *      <li>a provider and tenant, then the provider and tenant in <code>target</code>
     *      are matched against;
     *      <li>a provider, tenant and instance type, then all three of these in <code>target</code>
     *      are matched against;
     *  </ul>
     *  Note that unique names are ignored.
     */
    public boolean matchesInUse(ResourceType target)
    {
        return matches(target);
    }

    /** Uses the same rules as {@link #matchesInUse}, with the additional rule that the match is failed if
     *  {@link ResourceType#getUniqueName} is present. See {@link ResourceLimiter} for why this is needed.
     */
    public boolean matchesRequested(ResourceType target)
    {
        return target.getUniqueName().isEmpty() && matches(target);
    }
}
