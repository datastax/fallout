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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.ops.MultiConfigurationManagerTest.ConfigurationManagerOperation.*;
import static com.datastax.fallout.ops.NodeGroupHelpers.assertSuccessfulTransition;
import static com.datastax.fallout.ops.NodeGroupHelpers.waitForTransition;

public class MultiConfigurationManagerTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    enum ConfigurationManagerOperation
    {
        CONFIGURE, START, UNCONFIGURE, STOP, PREPARE_ARTIFACTS, COLLECT_ARTIFACTS
    }

    static class FailingConfigurationManager extends FakeConfigurationManager
    {
        private final ConfigurationManagerOperation failingOperation;

        private FailingConfigurationManager(ConfigurationManagerOperation failingOperation)
        {
            this.failingOperation = failingOperation;
        }

        @Override
        protected boolean configureImpl(NodeGroup nodeGroup)
        {
            return failingOperation != CONFIGURE;
        }

        @Override
        protected boolean startImpl(NodeGroup nodeGroup)
        {
            return failingOperation != START;
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
        public boolean prepareArtifactsImpl(Node node)
        {
            return failingOperation != PREPARE_ARTIFACTS;
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
        protected boolean configureImpl(NodeGroup nodeGroup)
        {
            calledOperations.add(CONFIGURE);
            return true;
        }

        @Override
        protected boolean startImpl(NodeGroup nodeGroup)
        {
            calledOperations.add(START);
            return true;
        }

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
        public boolean prepareArtifactsImpl(Node node)
        {
            calledOperations.add(PREPARE_ARTIFACTS);
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
        final ConfigurationManager failingConfigurationManager1 = new FailingConfigurationManager(failingOperation);
        final ConfigurationManager failingConfigurationManager2 = new FailingConfigurationManager(failingOperation);

        final MultiConfigurationManager multiConfigurationManager =
            new MultiConfigurationManager(
                List.of(failingConfigurationManager1, mockConfigurationManager, failingConfigurationManager2),
                new WritablePropertyGroup());

        return NodeGroupBuilder.create()
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(multiConfigurationManager)
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1)
            .withName("test")
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .build();
    }

    @BeforeEach
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
    public void a_failed_prepareArtifacts_does_not_prevent_other_prepareArtifacts()
    {
        NodeGroup nodeGroup = makeNodeGroupWithConfigurationManagerThatFailsAt(PREPARE_ARTIFACTS);

        assertSuccessfulTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_RUNNING);
        assertThat(nodeGroup.prepareArtifacts().join()).isFalse();
        assertThat(mockConfigurationManager.calledOperations).contains(PREPARE_ARTIFACTS);
    }

    @Test
    public void a_failed_collectArtifacts_does_not_prevent_other_collectArtifacts()
    {
        NodeGroup nodeGroup = makeNodeGroupWithConfigurationManagerThatFailsAt(COLLECT_ARTIFACTS);

        assertSuccessfulTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_CONFIGURED);
        assertThat(nodeGroup.collectArtifacts().join()).isFalse();
        assertThat(mockConfigurationManager.calledOperations).contains(COLLECT_ARTIFACTS);
    }

    @Test
    public void a_failed_configure_prevents_other_configures()
    {
        NodeGroup nodeGroup = makeNodeGroupWithConfigurationManagerThatFailsAt(CONFIGURE);

        assertThat(waitForTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_CONFIGURED)).wasNotSuccessful();
        assertThat(mockConfigurationManager.calledOperations).doesNotContain(CONFIGURE);
    }

    @Test
    public void a_failed_start_prevents_other_starts()
    {
        NodeGroup nodeGroup = makeNodeGroupWithConfigurationManagerThatFailsAt(START);

        assertThat(waitForTransition(nodeGroup, NodeGroup.State.STARTED_SERVICES_RUNNING)).wasNotSuccessful();
        assertThat(mockConfigurationManager.calledOperations).doesNotContain(START);
    }
}
