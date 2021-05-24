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

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;

import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.ops.ResourceType;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ScopedLogger;

/** We have to make sure we only reserve one {@link ResourceType} at a time; for example, if we want to reserve
 *  some instances on the FOO tenant of the BAR provider, we need to prevent anything else from
 *  reserving on that tenant at the same time, otherwise we could get resources being double-booked. */
public class ResourceReservationLocks
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(ResourceReservationLocks.class);

    private Set<ResourceType> activeResources = new HashSet<>();

    public class Lock
    {
        private Set<ResourceType> lockedResources;
        private final Instant acquiredAt = Instant.now();
        private Duration releasedAfterDuration;

        Lock(Set<ResourceType> resourceProviders)
        {
            lockedResources = resourceProviders;
        }

        /** Release the lock and return how long it was held for (for diagnostic purposes) */
        public synchronized Duration release()
        {
            if (releasedAfterDuration == null)
            {
                releaseLockedResources(lockedResources);
                releasedAfterDuration = Duration.between(acquiredAt, Instant.now());
            }
            return releasedAfterDuration;
        }

        public Set<ResourceType> getLockedResources()
        {
            return lockedResources;
        }
    }

    private synchronized Optional<Lock> tryAcquire(Set<ResourceType> required)
    {
        return logger.withScopedDebug("tryAcquire({})  active: {}", required, activeResources)
            .get(() -> {
                if (!couldAcquire(required))
                {
                    return Optional.empty();
                }

                activeResources.addAll(required);

                return Optional.of(new Lock(required));
            });
    }

    public Optional<Lock> tryAcquire(TestRun testRun)
    {
        return tryAcquire(
            ResourceRequirement.reservationLockResourceTypes(testRun.getResourceRequirements().stream()));
    }

    private synchronized boolean couldAcquire(Set<ResourceType> required)
    {
        return logger.withScopedDebug("couldAcquire({})  active: {}", required, activeResources)
            .get(() -> Collections.disjoint(activeResources, required));
    }

    public boolean couldAcquire(ReadOnlyTestRun testRun)
    {
        return logger.withScopedDebug("couldAcquire({})", testRun.getShortName())
            .get(() -> couldAcquire(
                ResourceRequirement.reservationLockResourceTypes(testRun.getResourceRequirements().stream())));
    }

    private synchronized void releaseLockedResources(Set<ResourceType> lockedResources)
    {
        logger.withScopedDebug("releaseLockedResources({})  active: {}", lockedResources, activeResources)
            .run(() -> {
                Preconditions.checkArgument(activeResources.containsAll(lockedResources));

                activeResources.removeAll(lockedResources);

                Verify.verify(activeResources.stream().noneMatch(lockedResources::contains));
            });
    }
}
