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

import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.ops.ResourceRequirement;

import static org.assertj.core.api.Assertions.assertThat;

public class ResourceTypeEqualityTest
{
    private ResourceRequirement.ResourceType openstackProvider;
    private ResourceRequirement.ResourceType copyOpenstackProvider;
    private ResourceRequirement.ResourceType otherOpenstackProvider;
    private ResourceRequirement.ResourceType ironicProvider;
    private ResourceRequirement.ResourceType copyIronicProvider;
    private ResourceRequirement.ResourceType otherIronicProvider;
    private ResourceRequirement.ResourceType lastIronicProvider;

    @Before
    public void setup()
    {
        openstackProvider = openstack("test");
        copyOpenstackProvider = openstack("test");
        otherOpenstackProvider = openstack("performance");

        ironicProvider = ironic("everyone", "bm2.small");
        copyIronicProvider = ironic("everyone", "bm2.small");
        otherIronicProvider = ironic("everyone", "perf-labv1");
        lastIronicProvider = ironic("dne", "perf-labvDNE");
    }

    private ResourceRequirement.ResourceType openstack(String tenant)
    {
        return new ResourceRequirement.ResourceType("openstack", tenant);
    }

    private ResourceRequirement.ResourceType ironic(String tenant, String instanceType)
    {
        return new ResourceRequirement.ResourceType("ironic", tenant, instanceType);
    }

    @Test
    public void openstack_resource_providers_match_only_if_using_the_same_tenant()
    {
        assertThat(openstackProvider).isEqualTo(copyOpenstackProvider);
        assertThat(openstackProvider).isNotEqualTo(otherOpenstackProvider);
    }

    @Test
    public void ironic_resource_providers_match_only_if_using_the_same_tenant_and_machine()
    {
        assertThat(ironicProvider).isEqualTo(copyIronicProvider);
        assertThat(ironicProvider).isNotEqualTo(otherOpenstackProvider);
        assertThat(ironicProvider).isNotEqualTo(lastIronicProvider);
    }

    @Test
    public void resource_providers_with_different_cloud_providers_should_not_match()
    {
        assertThat(openstackProvider).isNotEqualTo(ironicProvider);
        assertThat(openstackProvider).isNotEqualTo(otherIronicProvider);
        assertThat(openstackProvider).isNotEqualTo(lastIronicProvider);
        assertThat(otherOpenstackProvider).isNotEqualTo(ironicProvider);
        assertThat(otherOpenstackProvider).isNotEqualTo(otherIronicProvider);
        assertThat(otherOpenstackProvider).isNotEqualTo(lastIronicProvider);
    }
}
