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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.configmanagement.FakeConfigurationManager;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;

import static com.datastax.fallout.ops.MultiConfigurationManagerTest.ConfigurationManagerOperation.COLLECT_ARTIFACTS;
import static com.datastax.fallout.ops.MultiConfigurationManagerTest.ConfigurationManagerOperation.STOP;
import static com.datastax.fallout.ops.MultiConfigurationManagerTest.ConfigurationManagerOperation.UNCONFIGURE;
import static com.datastax.fallout.ops.NodeGroupHelpers.assertSuccessfulTransition;
import static com.datastax.fallout.ops.NodeGroupHelpers.waitForTransition;
import static com.datastax.fallout.runner.CheckResourcesResultAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

public class MultiConfigurationManagerTest extends TestHelpers.FalloutTest
{
    enum ConfigurationManagerOperation
    {
        UNCONFIGURE, STOP, COLLECT_ARTIFACTS
    }

    static class StubConfigurationManager extends FakeConfigurationManager
    {
        private final ConfigurationManagerOperation failingOperation;

        private StubConfigurationManager(
            ConfigurationManagerOperation failingOperation)
        {
            this.failingOperation = failingOperation;
        }

        @Override
        protected boolean unconfigureImpl(NodeGroup nodeGroup)
        {
            return failingOperation != UNCONFIGURE;
        }

        @Override
        protected boolean stopImpl(NodeGroup nodeGroup)
        {
            return failingOperation != STOP;
        }

        @Override
        protected boolean collectArtifactsImpl(Node node)
        {
            return failingOperation != COLLECT_ARTIFACTS;
        }
    }

    static class MockConfigurationManager extends FakeConfigurationManager
    {
        private final List<ConfigurationManagerOperation> calledOperations = new ArrayList<>();

        @Override
        protected boolean unconfigureImpl(NodeGroup nodeGroup)
        {
            calledOperations.add(UNCONFIGURE);
            return true;
        }

        @Override
        protected boolean stopImpl(NodeGroup nodeGroup)
        {
            calledOperations.add(STOP);
            return true;
        }

        @Override
        protected boolean collectArtifactsImpl(Node node)
        {
            calledOperations.add(COLLECT_ARTIFACTS);
            return true;
        }
    }

    private MockConfigurationManager mockConfigurationManager;

    private NodeGroup makeNodeGroupWithConfigurationManagerThatFailsAt(ConfigurationManagerOperation failingOperation)
    {
        final ConfigurationManager stubConfigurationManager = new StubConfigurationManager(failingOperation);

        final MultiConfigurationManager multiConfigurationManager =
            new MultiConfigurationManager(ImmutableList.of(stubConfigurationManager, mockConfigurationManager),
                new WritablePropertyGroup());

        return NodeGroupBuilder.create()
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(multiConfigurationManager)
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1)
            .withName("test")
            .withLoggers(new JobConsoleLoggers())
            .withTestRunArtifactPath(testRunArtifactPath())
            .build();
    }

    @Before
    public void setup()
    {
        mockConfigurationManager = new MockConfigurationManager();
    }

    @Test
    public void a_failed_stop_does_not_prevent_other_stops()
    {
        NodeGroup nodeGroup = makeNodeGroupWithConfigurationManagerThatFailsAt(STOP);

        assertSuccessfulTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_RUNNING);
        assertThat(waitForTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_CONFIGURED)).wasNotSuccessful();
        assertThat(mockConfigurationManager.calledOperations).contains(STOP);
    }

    @Test
    public void a_failed_unconfigure_does_not_prevent_other_unconfigures()
    {
        NodeGroup nodeGroup = makeNodeGroupWithConfigurationManagerThatFailsAt(UNCONFIGURE);

        assertSuccessfulTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_CONFIGURED);
        assertThat(waitForTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_UNCONFIGURED)).wasNotSuccessful();
        assertThat(mockConfigurationManager.calledOperations).contains(UNCONFIGURE);
    }

    @Test
    public void a_failed_collectArtifacts_does_not_prevent_other_collectArtifacts()
    {
        NodeGroup nodeGroup = makeNodeGroupWithConfigurationManagerThatFailsAt(COLLECT_ARTIFACTS);

        assertSuccessfulTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_CONFIGURED);
        assertThat(nodeGroup.collectArtifacts().join()).isFalse();
        assertThat(mockConfigurationManager.calledOperations).contains(COLLECT_ARTIFACTS);
    }
}
