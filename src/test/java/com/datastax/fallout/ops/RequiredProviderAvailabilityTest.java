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

import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.provisioner.LocalProvisioner;

import static com.datastax.fallout.runner.CheckResourcesResultAssert.assertThat;

public class RequiredProviderAvailabilityTest extends TestHelpers.FalloutTest
{
    static class ProvidingConfigurationManager extends ConfigurationManager
    {
        @Override
        public String prefix()
        {
            return "fallout_test.configuration_manager.provides.";
        }

        @Override
        public String name()
        {
            return "provides";
        }

        @Override
        public String description()
        {
            return "Test configuration manager which provides a provider";
        }

        @Override
        public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup properties)
        {
            return ImmutableSet.of(TestProvider.class);
        }

        @Override
        public boolean registerProviders(Node node)
        {
            new TestProvider(node);
            return true;
        }
    }

    static class TestProvider extends Provider
    {
        TestProvider(Node node)
        {
            super(node);
        }

        @Override
        public String name()
        {
            return "test_provider";
        }

        boolean requiredFunctionality()
        {
            return true;
        }
    }

    static class RequiringConfigurationManager extends ConfigurationManager
    {
        @Override
        public String prefix()
        {
            return "fallout_test.configuration_manager.requires.";
        }

        @Override
        public String name()
        {
            return "requires";
        }

        @Override
        public String description()
        {
            return "Test configuration manager which requires functionality from a provider";
        }

        @Override
        public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup properties)
        {
            return ImmutableSet.of(TestProvider.class);
        }

        @Override
        public boolean configureImpl(NodeGroup nodeGroup)
        {
            return nodeGroup.waitForAllNodes(n -> n.getProvider(TestProvider.class).requiredFunctionality(),
                "using required functionality");
        }
    }

    @Test
    public void configuration_managers_can_use_functionality_of_available_providers()
    {
        MultiConfigurationManager mcm = new MultiConfigurationManager(
            ImmutableList.of(new ProvidingConfigurationManager(), new RequiringConfigurationManager()),
            new WritablePropertyGroup());
        NodeGroup testGroup = NodeGroupBuilder.create()
            .withProvisioner(new LocalProvisioner())
            .withConfigurationManager(mcm)
            .withPropertyGroup(new WritablePropertyGroup())
            .withName("test-group")
            .withNodeCount(1)
            .withTestRunArtifactPath(testRunArtifactPath())
            .build();
        assertThat(testGroup.transitionState(NodeGroup.State.STARTED_SERVICES_CONFIGURED).join()).wasSuccessful();
    }
}
