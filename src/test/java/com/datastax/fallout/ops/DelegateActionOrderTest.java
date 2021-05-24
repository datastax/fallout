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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.common.provisioner.LocalProvisioner;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class DelegateActionOrderTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    List<ActionMethod> executionOrder = new ArrayList<>();

    private enum ActionMethod
    {
        DEPENDANT_CONFIGURES,
        DEPENDANT_STARTS,
        DEPENDANT_STOPS,
        DEPENDANT_UNCONFIGURES,
        PROVIDES_CONFIGURES,
        PROVIDES_STARTS,
        PROVIDES_STOPS,
        PROVIDES_UNCONFIGURES
    }

    private class DependantConfigurationManager extends ConfigurationManager
    {
        @Override
        public String prefix()
        {
            return "com.fallout.test.configuration_manager.dependant";
        }

        @Override
        public String name()
        {
            return "dependant";
        }

        @Override
        public String description()
        {
            return "Depends on Provides Configuration Manager.";
        }

        @Override
        public Set<Class<? extends Provider>> getRequiredProviders(PropertyGroup properties)
        {
            return Set.of(DependencyProvider.class);
        }

        @Override
        protected boolean configureImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.DEPENDANT_CONFIGURES);
            return true;
        }

        @Override
        protected boolean startImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.DEPENDANT_STARTS);
            return true;
        }

        @Override
        protected boolean stopImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.DEPENDANT_STOPS);
            return true;
        }

        @Override
        protected boolean unconfigureImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.DEPENDANT_UNCONFIGURES);
            return true;
        }
    }

    private class ProvidesConfigurationManager extends ConfigurationManager
    {
        @Override
        public String prefix()
        {
            return "com.fallout.test.configuration_manager.provides";
        }

        @Override
        public String name()
        {
            return "provides";
        }

        @Override
        public String description()
        {
            return "Provides for Dependant Configuration Manager";
        }

        @Override
        public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup properties)
        {
            return Set.of(DependencyProvider.class);
        }

        @Override
        protected boolean configureImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.PROVIDES_CONFIGURES);
            return true;
        }

        @Override
        protected boolean startImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.PROVIDES_STARTS);
            return true;
        }

        @Override
        protected boolean stopImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.PROVIDES_STOPS);
            return true;
        }

        @Override
        protected boolean unconfigureImpl(NodeGroup nodeGroup)
        {
            executionOrder.add(ActionMethod.PROVIDES_UNCONFIGURES);
            return true;
        }
    }

    static class DependencyProvider extends Provider
    {
        protected DependencyProvider(Node node)
        {
            super(node);
        }

        @Override
        public String name()
        {
            return "dependency";
        }
    }

    @Test
    public void delegates_are_configured_in_dependency_order_and_unconfigured_in_dependency_order()
    {
        MultiConfigurationManager mcm = new MultiConfigurationManager(
            List.of(new ProvidesConfigurationManager(), new DependantConfigurationManager()),
            new WritablePropertyGroup());
        NodeGroup testGroup = NodeGroupBuilder.create()
            .withProvisioner(new LocalProvisioner())
            .withConfigurationManager(mcm)
            .withPropertyGroup(new WritablePropertyGroup())
            .withName("test-group")
            .withNodeCount(1)
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .build();

        assertThat(testGroup.transitionState(NodeGroup.State.STARTED_SERVICES_RUNNING).join()).wasSuccessful();
        assertThat(testGroup.transitionState(NodeGroup.State.DESTROYED).join()).wasSuccessful();

        assertThat(executionOrder).containsExactly(
            ActionMethod.PROVIDES_CONFIGURES,
            ActionMethod.DEPENDANT_CONFIGURES,
            ActionMethod.PROVIDES_STARTS,
            ActionMethod.DEPENDANT_STARTS,
            ActionMethod.DEPENDANT_STOPS,
            ActionMethod.PROVIDES_STOPS,
            ActionMethod.DEPENDANT_UNCONFIGURES,
            ActionMethod.PROVIDES_UNCONFIGURES);
    }
}
