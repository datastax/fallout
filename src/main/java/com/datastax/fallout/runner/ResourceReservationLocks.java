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
package com.datastax.fallout.runner;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;

public class ResourceReservationLocks
{
    private static final Logger logger = LoggerFactory.getLogger(ResourceReservationLocks.class);

    private Set<ResourceRequirement.ResourceType> activeResources = new HashSet<>();

    public class Lock
    {
        private Set<ResourceRequirement.ResourceType> lockedResources;
        private final Instant acquiredAt = Instant.now();
        private Duration releasedAfterDuration;

        Lock(Set<ResourceRequirement.ResourceType> resourceProviders)
        {
            lockedResources = resourceProviders;
        }

        /** Release the lock and return how long it was held for (for diagnostic purposes) */
        public synchronized Duration release()
        {
            if (releasedAfterDuration == null)
            {
                logger.debug("Attempting to release. Active: {}\tLocked: {}", activeResources, lockedResources);
                releaseLockedResources(lockedResources);
                releasedAfterDuration = Duration.between(acquiredAt, Instant.now());
            }
            return releasedAfterDuration;
        }

        public Set<ResourceRequirement.ResourceType> getLockedResources()
        {
            return lockedResources;
        }
    }

    private synchronized Optional<Lock> tryAcquire(Set<ResourceRequirement.ResourceType> required)
    {
        logger.debug("Attempting to acquire. Active: {}\tRequired: {}", activeResources, required);
        if (!couldAcquire(required))
        {
            return Optional.empty();
        }

        activeResources.addAll(required);

        return Optional.of(new Lock(required));
    }

    public Optional<Lock> tryAcquire(TestRun testRun)
    {
        return tryAcquire(
            ResourceRequirement.reservationLockResourceTypes(testRun.getResourceRequirements().stream()));
    }

    private synchronized boolean couldAcquire(Set<ResourceRequirement.ResourceType> required)
    {
        return Collections.disjoint(activeResources, required);
    }

    public boolean couldAcquire(ReadOnlyTestRun testRun)
    {
        return couldAcquire(
            ResourceRequirement.reservationLockResourceTypes(testRun.getResourceRequirements().stream()));
    }

    private synchronized void releaseLockedResources(Set<ResourceRequirement.ResourceType> lockedResources)
    {
        Preconditions.checkArgument(activeResources.containsAll(lockedResources));

        activeResources.removeAll(lockedResources);

        Verify.verify(activeResources.stream().noneMatch(lockedResources::contains));
    }
}
