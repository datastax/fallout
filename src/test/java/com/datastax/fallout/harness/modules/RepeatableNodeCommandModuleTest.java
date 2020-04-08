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
package com.datastax.fallout.harness.modules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.ActiveTestRunBuilder;
import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.MockCommandExecutor;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.harness.TestDefinition;
import com.datastax.fallout.harness.TestResult;
import com.datastax.fallout.harness.TestRunnerTestHelpers;
import com.datastax.fallout.harness.checkers.FakeChecker;
import com.datastax.fallout.harness.impl.FakeModule;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.Product;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.ops.configmanagement.FakeConfigurationManager;
import com.datastax.fallout.ops.providers.NodeInfoProvider;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;

import static com.datastax.fallout.harness.Operation.Type.invoke;
import static com.datastax.fallout.harness.Operation.Type.ok;
import static com.datastax.fallout.harness.TestResultAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class RepeatableNodeCommandModuleTest extends EnsembleFalloutTest
{
    private static final String REPEAT_MODULE_1 = "repeat_1";
    private static final String REPEAT_MODULE_2 = "repeat_2";
    private static final String RUN_ONCE_MODULE = "noop";

    private MockCommandExecutor mockCommandExecutor;

    @Before
    public void setup()
    {
        mockCommandExecutor = new MockCommandExecutor();
    }

    class Repeater extends RepeatableNodeCommandModule<NodeInfoProvider>
    {
        private static final String NAME = "repeater";
        private static final String PREFIX = "test.module." + NAME + ".";

        public Repeater()
        {
            super(NodeInfoProvider.class, PREFIX);
        }

        @Override
        protected String commandDescription()
        {
            return "";
        }

        @Override
        protected NodeResponse runCommand(NodeInfoProvider provider, String command)
        {
            return mockCommandExecutor.executeLocally(logger(), command);
        }

        @Override
        public List<Product> getSupportedProducts()
        {
            return Product.everything();
        }

        @Override
        public String prefix()
        {
            return PREFIX;
        }

        @Override
        public String name()
        {
            return NAME;
        }

        @Override
        public String description()
        {
            return "Repeater module";
        }
    }

    private void runAndCheck(Map<String, Object> templateParams,
        Consumer<TestRunnerTestHelpers.MockingComponentFactory> componentFactoryModifier,
        Consumer<Collection<Operation>> check)
    {
        final TestRunnerTestHelpers.MockingComponentFactory componentFactory =
            new TestRunnerTestHelpers.MockingComponentFactory()
                .mockAll(Provisioner.class, FakeProvisioner::new)
                .mockAll(ConfigurationManager.class, FakeConfigurationManager::new)
                .mockAll(Checker.class, () -> new FakeChecker()
                {
                    @Override
                    public boolean check(Ensemble ensemble, Collection<Operation> history)
                    {
                        check.accept(history);
                        return true;
                    }
                });

        componentFactoryModifier.accept(componentFactory);

        final ActiveTestRunBuilder activeTestRunBuilder = createActiveTestRunBuilder()
            .withComponentFactory(componentFactory);

        final String yaml = getTestClassResource("concurrent-commands.yaml");
        final String expandedYaml = TestDefinition.expandTemplate(yaml, templateParams);

        final ActiveTestRun activeTestRun = activeTestRunBuilder
            .withEnsembleFromYaml(expandedYaml)
            .withWorkloadFromYaml(expandedYaml)
            .build();

        final CompletableFuture<TestResult> result = CompletableFuture
            .supplyAsync(() -> performTestRun(activeTestRun));

        assertThatCode(() -> result.get(20, TimeUnit.SECONDS))
            .doesNotThrowAnyException();

        assertThat(result.join()).isValid();
    }

    private void runAndCheck(Consumer<Collection<Operation>> check)
    {
        runAndCheck(Collections.emptyMap(),
            componentFactory -> componentFactory
                .mockNamed(Module.class, REPEAT_MODULE_1, Repeater::new)
                .mockNamed(Module.class, REPEAT_MODULE_2, Repeater::new),
            check);
    }

    private void runAndCheck(Map<String, Object> templateParams, Consumer<Collection<Operation>> check)
    {
        runAndCheck(templateParams,
            componentFactory -> componentFactory
                .mockNamed(Module.class, REPEAT_MODULE_1, Repeater::new)
                .mockNamed(Module.class, REPEAT_MODULE_2, Repeater::new),
            check);
    }

    @Test
    public void concurrent_commands_with_phase_lifetime_and_zero_iterations_execute_immediately_exactly_once()
    {
        runAndCheck(
            history -> {
                assertThat(
                    history.stream()
                        .filter(op -> op.getType() == invoke)
                        .map(Operation::getProcess))
                            .containsExactlyInAnyOrder(REPEAT_MODULE_1, REPEAT_MODULE_2);
            });
    }

    @Test
    public void concurrent_commands_with_phase_lifetime_and_some_iterations_execute_immediately_exactly_once()
    {
        final int repeat1Iterations = 4;
        final int repeat2Iterations = 5;

        runAndCheck(
            ImmutableMap.of("repeat_modules", ImmutableList.of(
                ImmutableMap.of("name", REPEAT_MODULE_1, "iterations", repeat1Iterations),
                ImmutableMap.of("name", REPEAT_MODULE_2, "iterations", repeat2Iterations))),
            history -> {
                assertThat(
                    history.stream()
                        .filter(op -> op.getType() == invoke)
                        .map(Operation::getProcess))
                            .containsExactlyInAnyOrder(REPEAT_MODULE_1, REPEAT_MODULE_2);
            });
    }

    @Test
    public void concurrent_commands_with_run_once_lifetime_and_zero_iterations_execute_exactly_once()
    {
        runAndCheck(
            ImmutableMap.of(
                "run_once_lifetime", true,
                "no_delay", true),
            history -> {
                assertThat(
                    history.stream()
                        .filter(op -> op.getType() == invoke)
                        .map(Operation::getProcess))
                            .containsExactlyInAnyOrder(REPEAT_MODULE_1, REPEAT_MODULE_2);
            });
    }

    @Test
    public void concurrent_commands_with_run_once_lifetime_and_some_iterations_execute_for_the_number_of_iterations()
    {
        final int repeat1Iterations = 4;
        final int repeat2Iterations = 5;

        runAndCheck(
            ImmutableMap.of(
                "run_once_lifetime", true,
                "no_delay", true,
                "repeat_modules", ImmutableList.of(
                    ImmutableMap.of("name", REPEAT_MODULE_1, "iterations", repeat1Iterations),
                    ImmutableMap.of("name", REPEAT_MODULE_2, "iterations", repeat2Iterations))),
            history -> {
                assertThat(repeatInvocationCounts(history))
                    .isEqualTo(ImmutableMap.of(
                        REPEAT_MODULE_1, (long) repeat1Iterations,
                        REPEAT_MODULE_2, (long) repeat2Iterations));
            });
    }

    @Test
    public void
        concurrent_commands_with_phase_lifetime_in_the_same_phase_as_a_run_once_module_that_terminates_immediately_execute_exactly_once()
    {
        runAndCheck(
            ImmutableMap.of(
                "with_run_once_module", true,
                "no_delay", true),
            history -> {
                assertThat(
                    history.stream()
                        .filter(op -> op.getType() == invoke)
                        .map(Operation::getProcess))
                            .containsExactlyInAnyOrder(RUN_ONCE_MODULE, REPEAT_MODULE_1, REPEAT_MODULE_2);
            });
    }

    private static boolean isRepeatInvocation(Operation op)
    {
        return op.getType() == invoke && op.getProcess().startsWith("repeat");
    }

    private static Map<String, Long> repeatInvocationCounts(Collection<Operation> history)
    {
        return history.stream()
            .filter(RepeatableNodeCommandModuleTest::isRepeatInvocation)
            .map(Operation::getProcess)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    private static long repeatInvocationsBeforeRunOnceModule(Collection<Operation> history)
    {
        return StreamUtils
            .takeUntil(
                history.stream()
                    .filter(op -> isRepeatInvocation(op) ||
                        (op.getType() == ok && op.getProcess().equals(RUN_ONCE_MODULE)))
                    .map(Operation::getProcess),
                process -> process.equals(RUN_ONCE_MODULE))
            .count();
    }

    private Collection<Long> repeatInvocationsCountsAfterRunOnceModule(Collection<Operation> history)
    {
        return StreamUtils
            .skipUntilInclusive(
                history.stream()
                    .filter(op -> isRepeatInvocation(op) ||
                        (op.getType() == ok && op.getProcess().equals(RUN_ONCE_MODULE)))
                    .map(Operation::getProcess),
                process -> process.equals(RUN_ONCE_MODULE))
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
            .values();
    }

    private void runConcurrentCommandsInSamePhaseAsRunOnceModule(
        int repeat1InvocationsBeforeRunOnceCompletes,
        int repeat2InvocationsBeforeRunOnceCompletes,
        int delayBeforeRunOnceCompletes,
        Map<String, Object> templateParams,
        Consumer<Collection<Operation>> check)
    {
        final int REPEAT_MODULES = 2;
        final List<CountDownLatch> latches = new ArrayList<>(REPEAT_MODULES);

        Function<Integer, Module> createRepeater = invocationsBeforeRunOnceCompletes -> {
            final CountDownLatch latch = new CountDownLatch(invocationsBeforeRunOnceCompletes);
            latches.add(latch);
            return new Repeater()
            {
                @Override
                protected NodeResponse runCommand(NodeInfoProvider provider, String command)
                {
                    latch.countDown();
                    return super.runCommand(provider, command);
                }
            };
        };

        runAndCheck(
            ImmutableMap.<String, Object>builder()
                .put("with_run_once_module", true)
                .put("no_delay", true)
                .putAll(templateParams)
                .build(),

            componentFactory -> componentFactory
                .mockNamed(Module.class, REPEAT_MODULE_1,
                    () -> createRepeater.apply(repeat1InvocationsBeforeRunOnceCompletes))
                .mockNamed(Module.class, REPEAT_MODULE_2,
                    () -> createRepeater.apply(repeat2InvocationsBeforeRunOnceCompletes))

                // The single noop module will wait until each of the reeat modules has run for
                // repeat*InvocationsBeforeRunOnceCompletes
                .mockNamed(Module.class, RUN_ONCE_MODULE, () -> new FakeModule()
                {
                    @Override
                    public void run(Ensemble ensemble, PropertyGroup properties)
                    {
                        emit(invoke);
                        latches.forEach(Uninterruptibles::awaitUninterruptibly);
                        Uninterruptibles.sleepUninterruptibly(delayBeforeRunOnceCompletes, TimeUnit.SECONDS);
                        emit(ok);
                    }
                }),

            history -> {
                // There should be exactly one complete noop module operation _after_
                // repeat1InvocationsBeforeRunOnceCompletes + repeat2InvocationsBeforeRunOnceCompletes
                assertThat(repeatInvocationsBeforeRunOnceModule(history))
                    .isEqualTo(repeat1InvocationsBeforeRunOnceCompletes + repeat2InvocationsBeforeRunOnceCompletes);

                assertThat(
                    history.stream().filter(op -> op.getType() == ok && op.getProcess().equals(RUN_ONCE_MODULE)))
                        .hasSize(1);

                // There should be no more than one of each repeat_* module invocation after the complete noop
                assertThat(repeatInvocationsCountsAfterRunOnceModule(history))
                    .allSatisfy(count -> assertThat(count).isBetween(0L, 1L));

                check.accept(history);
            });
    }

    private void runConcurrentCommandsInSamePhaseAsRunOnceModule(
        int repeat1InvocationsBeforeRunOnceCompletes,
        int repeat2InvocationsBeforeRunOnceCompletes,
        Map<String, Object> templateParams)
    {
        runConcurrentCommandsInSamePhaseAsRunOnceModule(
            repeat1InvocationsBeforeRunOnceCompletes,
            repeat2InvocationsBeforeRunOnceCompletes,
            0,
            templateParams,
            history -> {});
    }

    @Test
    public void
        concurrent_commands_with_phase_lifetime_and_zero_iterations_in_the_same_phase_as_a_run_once_module_execute_until_the_run_once_module_completes()
    {
        final int repeatInvocationsBeforeRunOnceComplete = 2;

        runConcurrentCommandsInSamePhaseAsRunOnceModule(
            repeatInvocationsBeforeRunOnceComplete,
            repeatInvocationsBeforeRunOnceComplete,
            Collections.emptyMap());
    }

    @Test
    public void
        concurrent_commands_with_phase_lifetime_and_some_iterations_in_the_same_phase_as_a_run_once_module_stop_executing_if_the_run_once_module_completes()
    {
        final int repeatInvocationsBeforeRunOnceComplete = 2;
        final int repeat1Iterations = 4;
        final int repeat2Iterations = 5;

        runConcurrentCommandsInSamePhaseAsRunOnceModule(
            repeatInvocationsBeforeRunOnceComplete,
            repeatInvocationsBeforeRunOnceComplete,

            ImmutableMap.of("repeat_modules", ImmutableList.of(
                ImmutableMap.of("name", REPEAT_MODULE_1, "iterations", repeat1Iterations),
                ImmutableMap.of("name", REPEAT_MODULE_2, "iterations", repeat2Iterations))));
    }

    @Test
    public void
        concurrent_commands_with_phase_lifetime_and_some_iterations_in_the_same_phase_as_a_run_once_module_execute_for_the_requested_iterations_if_the_run_once_module_does_not_complete()
    {
        final int repeat1Iterations = 4;
        final int repeat2Iterations = 5;

        runConcurrentCommandsInSamePhaseAsRunOnceModule(
            repeat1Iterations,
            repeat2Iterations,
            2,

            ImmutableMap.of("repeat_modules", ImmutableList.of(
                ImmutableMap.of("name", REPEAT_MODULE_1, "iterations", repeat1Iterations),
                ImmutableMap.of("name", REPEAT_MODULE_2, "iterations", repeat2Iterations))),

            history -> {
                assertThat(repeatInvocationCounts(history))
                    .isEqualTo(ImmutableMap.of(
                        REPEAT_MODULE_1, (long) repeat1Iterations,
                        REPEAT_MODULE_2, (long) repeat2Iterations));
            });
    }
}
