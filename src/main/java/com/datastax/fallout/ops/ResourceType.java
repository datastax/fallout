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

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Preconditions;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

import static java.util.Comparator.comparing;
import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsFirst;

@UDT(name = "resourceType")
public class ResourceType implements Comparable<ResourceType>
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

    public ResourceType(String provider, @Nullable String tenant, @Nullable String instanceType,
        Optional<String> uniqueName)
    {
        this.provider = Preconditions.checkNotNull(provider);
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

    public ResourceType copyOnlyProvider()
    {
        return new ResourceType(provider, null, null, Optional.empty());
    }

    public ResourceType copyOnlyProviderAndTenant()
    {
        return new ResourceType(provider, tenant, null, Optional.empty());
    }

    public ResourceType copyWithoutUniqueName()
    {
        return new ResourceType(provider, tenant, instanceType, Optional.empty());
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

    public int compareTo(ResourceType other)
    {
        return comparing(ResourceType::getProvider)
            .thenComparing(ResourceType::getTenant, nullsFirst(naturalOrder()))
            .thenComparing(ResourceType::getInstanceType, nullsFirst(naturalOrder()))
            .thenComparing(type -> type.getUniqueName().orElse(null), nullsFirst(naturalOrder()))
            .compare(this, other);
    }
}
