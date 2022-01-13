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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.quicktheories.core.Gen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.ops.NodeGroup.State.DESTROYED;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.lists;

public class NodeGroupCheckStateTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    private static final Logger logger = LoggerFactory.getLogger(NodeGroupCheckStateTest.class);

    private NodeGroup.State checkState(NodeGroup.State provisionerState,
        List<NodeGroup.State> configurationManagerStates)
    {
        logger.info("=== checkState({}, {})", provisionerState, configurationManagerStates);

        class CheckStateCM extends FakeConfigurationManager
        {
            private final NodeGroup.State checkStateResult;

            private CheckStateCM(NodeGroup.State checkStateResult)
            {
                this.checkStateResult = checkStateResult;
            }

            @Override
            protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
            {
                return checkStateResult;
            }
        }

        final MultiConfigurationManager multiConfigurationManager = new MultiConfigurationManager(
            configurationManagerStates.stream()
                .map(state -> new CheckStateCM(state))
                .collect(Collectors.toList()),
            new WritablePropertyGroup());

        NodeGroup nodeGroup = NodeGroupBuilder.create()
            .withProvisioner(new FakeProvisioner() {
                @Override
                protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
                {
                    return provisionerState;
                }
            })
            .withConfigurationManager(multiConfigurationManager)
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1)
            .withName("test")
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .build();

        nodeGroup.checkState().join();

        return nodeGroup.getState();
    }

    private static Gen<NodeGroup.State> runLevelAndUnknownStates()
    {
        return arbitrary().pick(
            Arrays.stream(NodeGroup.State.values())
                .filter(state -> state.isRunLevelState() || state.isUnknownState())
                .toList());
    }

    private static Gen<NodeGroup.State> configManagementStates()
    {
        return arbitrary().pick(
            Arrays.stream(NodeGroup.State.values())
                .filter(NodeGroup.State::isConfigManagementState)
                .toList());
    }

    private static Gen<NodeGroup.State> nonConfigManagementStates()
    {
        return arbitrary().pick(
            Arrays.stream(NodeGroup.State.values())
                .filter(
                    state -> !state.isConfigManagementState() && (state.isRunLevelState() || state.isUnknownState()))
                .toList());
    }

    @Test
    public void
        checkState_returns_the_lowest_non_unknown_configuration_manager_state_when_provisioner_state_is_a_configManagement_state()
    {
        qt()
            .forAll(
                configManagementStates(),
                lists().of(runLevelAndUnknownStates()).ofSizeBetween(1, 6))
            .checkAssert((provisionerState, configurationManagerStates) -> {

                // Replace all unknown states with the provisioner state
                NodeGroup.State expectedState = configurationManagerStates.stream()
                    .filter(NodeGroup.State::isConfigManagementState)
                    .min(Comparator.comparing(NodeGroup.State::ordinal))
                    .orElse(provisionerState);

                assertThat(checkState(provisionerState, configurationManagerStates))
                    .isEqualTo(expectedState);
            });
    }

    @Test
    public void checkState_returns_the_provisioner_state_when_provisioner_state_is_not_a_configManagement_state()
    {
        qt()
            .forAll(
                nonConfigManagementStates(),
                lists().of(runLevelAndUnknownStates()).ofSizeBetween(1, 6))
            .checkAssert((provisionerState, configurationManagerStates) -> {

                // If the provisioner state is unknown, then it defaults to DESTROYED in the first instance
                NodeGroup.State expectedState = provisionerState.isUnknownState() ? DESTROYED : provisionerState;

                assertThat(checkState(provisionerState, configurationManagerStates))
                    .isEqualTo(expectedState);
            });
    }
}
