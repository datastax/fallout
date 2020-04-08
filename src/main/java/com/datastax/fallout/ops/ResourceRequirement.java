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

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

@UDT(name = "resourceRequirement")
public class ResourceRequirement
{
    @Field
    private ResourceType resourceType;

    @Field
    private ResourceType reservationLockResourceType;

    @Field
    private int nodeCount;

    public ResourceRequirement()
    {

    }

    public ResourceRequirement(ResourceType resourceType, int nodeCount)
    {
        this.resourceType = resourceType;
        this.nodeCount = nodeCount;
    }

    private ResourceRequirement(ResourceType resourceType, ResourceType reservationLockResourceType, int nodeCount)
    {
        this.resourceType = resourceType;
        this.reservationLockResourceType = reservationLockResourceType;
        this.nodeCount = nodeCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ResourceRequirement that = (ResourceRequirement) o;
        return nodeCount == that.nodeCount &&
            resourceType.equals(that.resourceType) &&
            Objects.equals(reservationLockResourceType, that.reservationLockResourceType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resourceType, reservationLockResourceType, nodeCount);
    }

    public int getNodeCount()
    {
        return nodeCount;
    }

    public void setReservationLockResourceType(ResourceType reservationLockResourceType)
    {
        this.reservationLockResourceType = reservationLockResourceType;
    }

    public ResourceType getResourceType()
    {
        return resourceType;
    }

    public ResourceType getReservationLockResourceType()
    {
        return reservationLockResourceType;
    }

    private ResourceRequirement copyWithNodeCount(int nodeCount)
    {
        return new ResourceRequirement(resourceType, reservationLockResourceType, nodeCount);
    }

    public ResourceRequirement copyWithoutUniqueName()
    {
        return new ResourceRequirement(resourceType.copyWithoutUniqueName(), nodeCount);
    }

    public static Set<ResourceRequirement> reducedResourceRequirements(Stream<ResourceRequirement> resourceRequirements)
    {
        return resourceRequirements.collect(
            Collectors.groupingBy(r -> r.copyWithNodeCount(0),
                Collectors.summingInt(ResourceRequirement::getNodeCount)))
            .entrySet()
            .stream()
            .map(e -> e.getKey().copyWithNodeCount(e.getValue()))
            .collect(Collectors.toSet());
    }

    public static Set<ResourceType> reservationLockResourceTypes(Stream<ResourceRequirement> resourceRequirements)
    {
        return resourceRequirements.map(ResourceRequirement::getReservationLockResourceType)
            .collect(Collectors.toSet());
    }

    @Override
    public String toString()
    {
        return "ResourceRequirement{" +
            "resourceType=" + resourceType +
            ", nodeCount=" + nodeCount +
            '}';
    }

    @UDT(name = "resourceType")
    public static class ResourceType
    {
        @Field
        private String provider;

        @Field
        private String tenant;

        @Field
        private String instanceType;

        @Field
        private Optional<String> uniqueName = Optional.empty();

        public ResourceType()
        {

        }

        public ResourceType(String provider, String tenant)
        {
            Preconditions.checkArgument(provider != null, "provider must not be null");
            Preconditions.checkArgument(tenant != null, "tenant must not be null");
            this.provider = provider;
            this.tenant = tenant;
            this.uniqueName = Optional.empty();
        }

        @VisibleForTesting
        public ResourceType(String provider, String tenant, String instanceType)
        {
            this(provider, tenant, instanceType, Optional.empty());
        }

        public ResourceType(String provider, String tenant, String instanceType, Optional<String> uniqueName)
        {
            Preconditions.checkArgument(provider != null, "provider must not be null");
            Preconditions.checkArgument(tenant != null, "tenant must not be null");
            Preconditions.checkArgument(instanceType != null, "instanceType must not be null");
            this.provider = provider;
            this.tenant = tenant;
            this.instanceType = instanceType;
            this.uniqueName = uniqueName;
        }

        public String getProvider()
        {
            return provider;
        }

        public String getTenant()
        {
            return tenant;
        }

        public String getInstanceType()
        {
            return instanceType;
        }

        public Optional<String> getUniqueName()
        {
            return uniqueName;
        }

        public ResourceType copyOnlyProviderAndTenant()
        {
            return new ResourceType(provider, tenant);
        }

        public ResourceType copyWithoutUniqueName()
        {
            return new ResourceType(provider, tenant, instanceType);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            ResourceType that = (ResourceType) o;
            return Objects.equals(provider, that.provider) &&
                Objects.equals(tenant, that.tenant) &&
                Objects.equals(instanceType, that.instanceType) &&
                Objects.equals(uniqueName, that.uniqueName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(provider, tenant, instanceType, uniqueName);
        }

        @Override
        public String toString()
        {
            String res = "ResourceType(" + provider + " " + tenant;
            if (instanceType != null)
            {
                res += ", instanceType=" + instanceType;
            }
            if (uniqueName.isPresent())
            {
                res += ", uniqueName=" + uniqueName.get();
            }
            return res + ")";
        }
    }
}
