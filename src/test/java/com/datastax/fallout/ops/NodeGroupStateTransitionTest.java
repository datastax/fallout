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
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.assertj.Assertions.assertThatExceptionOfType;
import static com.datastax.fallout.ops.NodeGroup.State.*;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.DOWN;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.UP;
import static com.datastax.fallout.ops.NodeGroupHelpers.waitForTransition;
import static com.datastax.fallout.ops.NodeGroupStateTransitionTest.Method.*;
import static com.datastax.fallout.util.Iterators.takeWhile;
import static java.lang.StrictMath.abs;

class NodeGroupStateTransitionTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    enum Method
    {
        PROVISIONER_RESERVE_IMPL,
        PROVISIONER_CREATE_IMPL,
        PROVISIONER_PREPARE_IMPL,
        PROVISIONER_START_IMPL,
        PROVISIONER_STOP_IMPL,
        PROVISIONER_DESTROY_IMPL,
        PROVISIONER_CHECK_STATE_IMPL,
        CONFIGURATION_MANAGER_CONFIGURE_IMPL,
        CONFIGURATION_MANAGER_UNCONFIGURE_IMPL,
        CONFIGURATION_MANAGER_START_IMPL,
        CONFIGURATION_MANAGER_STOP_IMPL,
        CONFIGURATION_MANAGER_CHECK_STATE_IMPL
    }

    private List<Method> seenCalls = new ArrayList<>();
    private List<Method> expectedCalls = new ArrayList<>();

    private static void addCall(List<Method> calls, Method method)
    {
        calls.add(method);
    }

    @AfterEach
    public void assertExpectedCallsSeen()
    {
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(seenCalls).containsExactlyElementsOf(expectedCalls);

            if (!softly.errorsCollected().isEmpty())
            {
                softly.fail("Expected calls not seen; all expected:\n%s\nall seen\n%s",
                    expectedCalls, seenCalls);
            }
        });
    }

    private void randomPause()
    {
        try
        {
            Thread.sleep(ThreadLocalRandom.current().nextLong(0, 10));
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    /** Record a call as having been seen.  This will be called by multiple
     *  MockConfigurationManagers running in parallel, so we must synchronize access */
    synchronized void call(Method method)
    {
        // Randomly pause before completing the call to mix up the ordering of calls between nodegroups
        randomPause();
        addCall(seenCalls, method);
    }

    void expect(Method method)
    {
        addCall(expectedCalls, method);
    }

    /** Uses Expectation to model the calls expected by an up or down transition to a particular State */
    private class Expectations
    {
        Optional<Method> up = Optional.empty();
        Optional<Method> down = Optional.empty();

        Expectations up(Method call)
        {
            this.up = Optional.of(call);
            return this;
        }

        Expectations down(Method call)
        {
            this.down = Optional.of(call);
            return this;
        }

        boolean maybeExpectCallForTransition(NodeGroup.State startState, NodeGroup.State endState)
        {
            return (startState.direction(endState) == UP ? up :
                startState.direction(endState) == DOWN ? down :
                Optional.<Method>empty())
                    .map(call -> { expect(call); return true; })
                    .orElse(false);
        }
    }

    Expectations expectations()
    {
        return new Expectations();
    }

    EnumMap<NodeGroup.State, Expectations> stateExpectations()
    {
        return new EnumMap<>(ImmutableMap.<NodeGroup.State, Expectations>builder()
            .put(DESTROYED, expectations()
                .down(PROVISIONER_DESTROY_IMPL))
            .put(RESERVED, expectations()
                .up(PROVISIONER_RESERVE_IMPL))
            .put(CREATED, expectations()
                .up(PROVISIONER_CREATE_IMPL))
            .put(STOPPED, expectations()
                .up(PROVISIONER_PREPARE_IMPL)
                .down(PROVISIONER_STOP_IMPL))
            .put(STARTED_SERVICES_UNCONFIGURED, expectations()
                .up(PROVISIONER_START_IMPL)
                .down(CONFIGURATION_MANAGER_UNCONFIGURE_IMPL))
            .put(STARTED_SERVICES_CONFIGURED, expectations()
                .up(CONFIGURATION_MANAGER_CONFIGURE_IMPL)
                .down(CONFIGURATION_MANAGER_STOP_IMPL))
            .put(STARTED_SERVICES_RUNNING, expectations()
                .up(CONFIGURATION_MANAGER_START_IMPL))
            .build());
    }

    void assertTransition(NodeGroup.State state, NodeGroup.State expectedEndState)
    {
        assertThat(nodeGroup.transitionState(state).join()).wasSuccessful();
        assertThat(nodeGroup).hasState(expectedEndState);
    }

    static final NodeGroup.State CHECK_STATE_RESULT = DESTROYED;

    class MockProvisioner extends FakeProvisioner
    {
        @Override
        protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
        {
            call(PROVISIONER_RESERVE_IMPL);
            return CheckResourcesResult.AVAILABLE;
        }

        @Override
        protected CheckResourcesResult createImpl(NodeGroup nodeGroup)
        {
            call(PROVISIONER_CREATE_IMPL);
            return CheckResourcesResult.AVAILABLE;
        }

        @Override
        protected boolean prepareImpl(NodeGroup nodeGroup)
        {
            call(PROVISIONER_PREPARE_IMPL);
            return true;
        }

        @Override
        protected boolean startImpl(NodeGroup nodeGroup)
        {
            call(PROVISIONER_START_IMPL);
            return true;
        }

        @Override
        protected boolean stopImpl(NodeGroup nodeGroup)
        {
            call(PROVISIONER_STOP_IMPL);
            return true;
        }

        @Override
        protected boolean destroyImpl(NodeGroup nodeGroup)
        {
            call(PROVISIONER_DESTROY_IMPL);
            return true;
        }

        @Override
        protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
        {
            call(PROVISIONER_CHECK_STATE_IMPL);
            return CHECK_STATE_RESULT;
        }
    }

    private class MockConfigurationManager extends FakeConfigurationManager
    {
        @Override
        protected boolean configureImpl(NodeGroup nodeGroup)
        {
            call(CONFIGURATION_MANAGER_CONFIGURE_IMPL);
            return true;
        }

        @Override
        protected boolean unconfigureImpl(NodeGroup nodeGroup)
        {
            call(CONFIGURATION_MANAGER_UNCONFIGURE_IMPL);
            return true;
        }

        @Override
        protected boolean startImpl(NodeGroup nodeGroup)
        {
            call(CONFIGURATION_MANAGER_START_IMPL);
            return true;
        }

        @Override
        protected boolean stopImpl(NodeGroup nodeGroup)
        {
            call(CONFIGURATION_MANAGER_STOP_IMPL);
            return true;
        }

        @Override
        protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
        {
            call(CONFIGURATION_MANAGER_CHECK_STATE_IMPL);
            return STARTED_SERVICES_UNCONFIGURED;
        }
    }

    protected Provisioner provisioner;

    private ConfigurationManager configurationManager;

    NodeGroup nodeGroup;

    private NodeGroup makeNodeGroup()
    {
        return NodeGroupBuilder
            .create()
            .withName("test-group")
            .withProvisioner(provisioner)
            .withConfigurationManager(configurationManager)
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(10)
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .build();
    }

    @BeforeEach
    public void setup()
    {
        provisioner = new MockProvisioner();
        configurationManager = new MockConfigurationManager();
        nodeGroup = makeNodeGroup();

        assertThat(nodeGroup).hasState(UNKNOWN);
    }

    void assertTransition(NodeGroup.State expectedEndState)
    {
        assertThat(nodeGroup.transitionState(expectedEndState).join()).wasSuccessful();
        assertThat(nodeGroup).hasState(expectedEndState);
    }

    static NodeGroup.State expectedCheckStateResult(NodeGroup.State currentState)
    {
        if (currentState.isUnknownState())
        {
            return currentState == FAILED ? STOPPED : DESTROYED;
        }
        return currentState;
    }

    /** Return a Stream of all runlevel states, either up or down, that will be reached between fromState and toState,
     *  not including fromState */
    static Stream<NodeGroup.State> toRunLevel(NodeGroup.State fromState, NodeGroup.State toState)
    {
        if (fromState.isUnknownState())
        {
            fromState = expectedCheckStateResult(fromState);
        }

        Preconditions.checkArgument(fromState.isRunLevelState());
        Preconditions.checkArgument(toState.isRunLevelState());

        final int increment = fromState.direction(toState) == UP ? 1 : -1;
        final int limit = fromState == toState ?
            0 :
            abs(toState.ordinal() - fromState.ordinal());

        return Stream
            .iterate(fromState.ordinal() + increment, stateOrdinal -> stateOrdinal + increment)
            .limit(limit)
            .map(stateOrdinal -> NodeGroup.State.values()[stateOrdinal])
            .filter(state -> increment > 0 || (state != CREATED && state != RESERVED));
    }

    NodeGroup.State expectedEndState(NodeGroup.State startState, NodeGroup.State endState)
    {
        if (startState.isUnknownState())
        {
            startState = CHECK_STATE_RESULT;
        }
        return toRunLevel(startState, endState).reduce((a, b) -> b).orElse(startState);
    }

    public static class SingleTests extends NodeGroupStateTransitionTest
    {
        @Test
        public void toRunLevel_validity()
        {
            assertThat(toRunLevel(DESTROYED, RESERVED))
                .containsExactly(RESERVED);

            assertThat(toRunLevel(STARTED_SERVICES_RUNNING, DESTROYED))
                .containsExactly(
                    STARTED_SERVICES_CONFIGURED,
                    STARTED_SERVICES_UNCONFIGURED,
                    STOPPED,
                    DESTROYED);

            assertThat(toRunLevel(DESTROYED, STARTED_SERVICES_RUNNING))
                .containsExactly(
                    RESERVED,
                    CREATED,
                    STOPPED,
                    STARTED_SERVICES_UNCONFIGURED,
                    STARTED_SERVICES_CONFIGURED,
                    STARTED_SERVICES_RUNNING);

            assertThat(toRunLevel(STOPPED, STARTED_SERVICES_UNCONFIGURED))
                .containsExactly(STARTED_SERVICES_UNCONFIGURED);

            assertThat(toRunLevel(STARTED_SERVICES_RUNNING, STARTED_SERVICES_RUNNING))
                .isEmpty();

            assertThat(toRunLevel(STARTED_SERVICES_CONFIGURED, CREATED))
                .containsExactly(
                    STARTED_SERVICES_UNCONFIGURED,
                    STOPPED);

            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> toRunLevel(CREATING, CREATED));

            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> toRunLevel(DESTROYED, CREATING));
        }

        @Test
        public void creation_transition()
        {
            assertTransition(CREATED);

            expect(PROVISIONER_CHECK_STATE_IMPL);
            expect(PROVISIONER_RESERVE_IMPL);
            expect(PROVISIONER_CREATE_IMPL);
        }

        @Test
        public void checkState_is_not_called_once_state_is_known()
        {
            assertTransition(DESTROYED);
            assertTransition(DESTROYED);

            expect(PROVISIONER_CHECK_STATE_IMPL);
        }

        @Test
        public void creation_is_idempotent()
        {
            assertTransition(CREATED);
            assertTransition(CREATED);

            expect(PROVISIONER_CHECK_STATE_IMPL);
            expect(PROVISIONER_RESERVE_IMPL);
            expect(PROVISIONER_CREATE_IMPL);
        }
    }

    public static class NodeGroupStateTransitionToFailedAndUnknownTest extends NodeGroupStateTransitionTest
    {
        public static Stream<NodeGroup.State> states()
        {
            return Arrays.stream(NodeGroup.State.values())
                .filter(NodeGroup.State::isRunLevelState);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("states")
        public void transition_to_failed_fails(NodeGroup.State state)
        {
            nodeGroup.setState(state);

            assertThat(nodeGroup.transitionState(FAILED).join()).wasNotSuccessful();

            assertThat(nodeGroup).hasState(state);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("states")
        public void transition_to_unknown_fails(NodeGroup.State state)
        {
            nodeGroup.setState(state);

            assertThat(nodeGroup.transitionState(UNKNOWN).join()).wasNotSuccessful();

            assertThat(nodeGroup).hasState(state);
        }
    }

    public static class TransitionLegalityTest extends NodeGroupStateTransitionTest
    {
        protected static class Transition
        {
            final boolean isLegal;
            final NodeGroup.State startState;
            final NodeGroup.State endState;

            public Transition(boolean isLegal, NodeGroup.State startState, NodeGroup.State endState)
            {
                this.isLegal = isLegal;
                this.startState = startState;
                this.endState = endState;
            }

            @Override
            public String toString()
            {
                return "Transition{" +
                    "isLegal=" + isLegal +
                    ", from=" + startState +
                    ", to=" + endState +
                    '}';
            }
        }

        static Stream<Transition> allTransitions()
        {
            final Stream<Pair<NodeGroup.State, NodeGroup.State>> illegalTransitionsFromRunLevelAndUnknownStates =
                Arrays.stream(NodeGroup.State.values())
                    .filter(startState -> startState.isRunLevelState() || startState.isUnknownState())
                    .flatMap(startState -> Arrays.stream(NodeGroup.State.values())
                        .filter(endState -> endState.isTransitioningState() || endState.isUnknownState())
                        .map(endState -> Pair.of(startState, endState)));

            final Stream<Pair<NodeGroup.State, NodeGroup.State>> illegalTransitionsFromTransitioningStates =
                Arrays.stream(NodeGroup.State.values())
                    .filter(NodeGroup.State::isTransitioningState)
                    .flatMap(startState -> Arrays.stream(NodeGroup.State.values())
                        .map(endState -> Pair.of(startState, endState)));

            final List<Pair<NodeGroup.State, NodeGroup.State>> illegalTransitions = Stream
                .concat(
                    illegalTransitionsFromRunLevelAndUnknownStates,
                    illegalTransitionsFromTransitioningStates)
                .collect(Collectors.toList());

            final Stream<Pair<NodeGroup.State, NodeGroup.State>> legalTransitions =
                Arrays.stream(NodeGroup.State.values())
                    .flatMap(startState -> Arrays.stream(NodeGroup.State.values())
                        .map(endState -> Pair.of(startState, endState)))
                    .filter(pair -> !illegalTransitions.contains(pair));

            return Stream
                .concat(
                    illegalTransitions.stream()
                        .map(pair -> new Transition(false, pair.getLeft(), pair.getRight())),
                    legalTransitions
                        .map(pair -> new Transition(true, pair.getLeft(), pair.getRight())));
        }
    }

    public static class IllegalStateTransitionTest extends TransitionLegalityTest
    {
        public static Stream<Transition> illegalTransitions()
        {
            return allTransitions().filter(transition -> !transition.isLegal);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("illegalTransitions")
        public void illegal_transitions_fail_and_leave_nodegroup_alone(Transition transition)
        {
            nodeGroup.setState(transition.startState);

            assertThat(waitForTransition(nodeGroup, transition.endState)).wasNotSuccessful();
            assertThat(nodeGroup).hasState(transition.startState);
        }
    }

    public static class LegalStateTransitionTest extends TransitionLegalityTest
    {
        public static Stream<Transition> legalTransitions()
        {
            return allTransitions().filter(transition -> transition.isLegal);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("legalTransitions")
        public void legal_transitions_work_and_invoke_the_expected_provisioner_and_configuration_manager_calls(
            Transition transition)
        {
            nodeGroup.setState(transition.startState);

            final EnumMap<NodeGroup.State, Expectations> stateExpectations = stateExpectations();

            // Get the states we expect to transition between for each pair of
            // (startState, endState), and create iterator.
            List<NodeGroup.State> expectedRunLevelStates = toRunLevel(transition.startState, transition.endState)
                .collect(Collectors.toList());
            ListIterator<NodeGroup.State> stateListIterator = expectedRunLevelStates.listIterator();

            // Expect checkState if we're in an unknown state
            if (transition.startState.isUnknownState())
            {
                expect(PROVISIONER_CHECK_STATE_IMPL);
            }

            final NodeGroup.State checkedStartState = expectedCheckStateResult(transition.startState);

            // Set expected node group transitions.
            takeWhile(stateListIterator, state -> stateExpectations.get(state)
                .maybeExpectCallForTransition(checkedStartState, transition.endState));

            // Expectations set: perform the transition
            assertTransition(transition.endState, expectedEndState(checkedStartState, transition.endState));
        }
    }
}
