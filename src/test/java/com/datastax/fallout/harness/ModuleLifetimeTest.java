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
package com.datastax.fallout.harness;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.components.impl.FakeModule;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.util.component_discovery.MockingComponentFactory;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.assertj.Assertions.assertThatCode;

public class ModuleLifetimeTest extends EnsembleFalloutTest<FalloutConfiguration>
{
    @Test
    public void run_to_end_of_phase_modules_run_until_run_once_modules_complete()
    {
        String yaml = readYamlFile("module-lifetime-fakes.yaml");

        final int RUN_ONCE_MODULES = 2;

        final CountDownLatch runOnceRunsStarted = new CountDownLatch(RUN_ONCE_MODULES);
        final CountDownLatch continueRunOnceRuns = new CountDownLatch(1);

        final int RUN_TO_END_OF_PHASE_MODULES = 2;
        final int RUN_TO_END_OF_PHASE_RUNS = 4;

        final CountDownLatch runToEndOfPhaseRunsCompleted = new CountDownLatch(RUN_TO_END_OF_PHASE_RUNS);

        final ActiveTestRunBuilder activeTestRunBuilder = createActiveTestRunBuilder()
            .withComponentFactory(new MockingComponentFactory()
                .mockNamed(Module.class, "run-once-fake", () -> new FakeModule() {
                    @Override
                    public void run(Ensemble ensemble, PropertyGroup properties)
                    {
                        runOnceRunsStarted.countDown();
                        Uninterruptibles.awaitUninterruptibly(continueRunOnceRuns);
                        super.run(ensemble, properties);
                    }
                })
                .mockNamed(Module.class, "run-to-end-of-phase-fake", () -> new FakeModule() {
                    @Override
                    public void run(Ensemble ensemble, PropertyGroup properties)
                    {
                        super.run(ensemble, properties);
                        runToEndOfPhaseRunsCompleted.countDown();
                    }
                }));

        final ActiveTestRun activeTestRun = activeTestRunBuilder
            .withTestDefinitionFromYaml(yaml)
            .build();

        final CompletableFuture<TestResult> result = CompletableFuture
            .supplyAsync(() -> performTestRun(activeTestRun));

        try
        {
            // Wait for fixed runs to start
            assertThat(Uninterruptibles.awaitUninterruptibly(runOnceRunsStarted, 10, TimeUnit.SECONDS)).isTrue();

            // Wait for at least RUN_TO_END_OF_PHASE_RUNS of the dynamic module to complete
            assertThat(Uninterruptibles.awaitUninterruptibly(runToEndOfPhaseRunsCompleted, 10, TimeUnit.SECONDS))
                .isTrue();
        }
        finally
        {
            continueRunOnceRuns.countDown();
        }

        assertThatCode(() -> result.get(10, TimeUnit.SECONDS))
            .doesNotThrowAnyException();

        final TestResult testResult = result.join();
        assertThat(testResult).isValid();

        List<String> invokeProcessOps = testResult.history().stream()
            .filter(op -> op.getType() == Operation.Type.invoke)
            .map(Operation::getProcess)
            .map(process -> process.replaceAll("-\\d$", ""))
            .toList();

        // We should have seen at least RUN_TO_END_OF_PHASE_RUNS invokes of the run-to-end-of-phase modules
        assertThat(
            invokeProcessOps.stream()
                .filter(process -> process.equals("run-to-end-of-phase"))
                .count())
                    .isGreaterThanOrEqualTo(RUN_TO_END_OF_PHASE_RUNS);

        // We should have seen exactly RUN_ONCE_MODULES invokes of the run-once modules
        assertThat(
            invokeProcessOps.stream()
                .filter(process -> process.equals("run-once"))
                .count())
                    .isEqualTo(RUN_ONCE_MODULES);

        // The run-to-end-of-phase modules must finish _after_ the run-once ones
        List<String> endProcessOps = testResult.history().stream()
            .filter(op -> op.getType() == Operation.Type.end)
            .map(Operation::getProcess)
            .map(process -> process.replaceAll("-\\d$", ""))
            .toList();

        List<String> expectedEndProcessOps = ImmutableList.<String>builder()
            .addAll(Collections.nCopies(RUN_ONCE_MODULES, "run-once"))
            .addAll(Collections.nCopies(RUN_TO_END_OF_PHASE_MODULES, "run-to-end-of-phase"))
            .build();

        assertThat(endProcessOps).isEqualTo(expectedEndProcessOps);
    }

    private static class PhaseLifetimeFake extends FakeModule
    {
        private PhaseLifetimeFake()
        {
            super(RunToEndOfPhaseMethod.AUTOMATIC);
        }
    }

    @Test
    public void exceptions_in_module_teardown_do_not_hang_phase_lifetime_modules()
    {
        String yaml = readYamlFile("phase-lifetime-fakes.yaml");

        AtomicBoolean tearDownCalled = new AtomicBoolean(false);

        final ActiveTestRunBuilder activeTestRunBuilder = createActiveTestRunBuilder()
            .withComponentFactory(new MockingComponentFactory()
                .mockNamed(Module.class, "fake", () -> new FakeModule() {
                    @Override
                    public void teardown(Ensemble ensemble, PropertyGroup properties)
                    {
                        super.teardown(ensemble, properties);
                        tearDownCalled.set(true);
                        throw new RuntimeException("bang!");
                    }
                })
                .mockNamed(Module.class, "phase-lifetime-fake", PhaseLifetimeFake::new));

        final ActiveTestRun activeTestRun = activeTestRunBuilder
            .withTestDefinitionFromYaml(yaml)
            .build();

        final CompletableFuture<TestResult> result = CompletableFuture
            .supplyAsync(() -> performTestRun(activeTestRun));

        assertThatCode(() -> result.get(10, TimeUnit.SECONDS))
            .doesNotThrowAnyException();

        assertThat(tearDownCalled.get()).isTrue();

        assertThat(result.join()).isNotValid();
    }

    @Test
    public void ending_a_module_before_phase_lifetime_module_starts_does_not_npe()
    {
        String yaml = readYamlFile("phase-lifetime-fakes.yaml");

        CountDownLatch moduleEnded = new CountDownLatch(1);
        AtomicReference<Module> runOnceModule = new AtomicReference<>();

        final ActiveTestRunBuilder activeTestRunBuilder = createActiveTestRunBuilder()
            .withComponentFactory(new MockingComponentFactory()
                .mockNamed(Module.class, "fake", () -> {
                    Module module = new FakeModule();
                    runOnceModule.set(module);
                    return module;
                })
                .mockNamed(Module.class, "phase-lifetime-fake", () -> new PhaseLifetimeFake() {
                    @Override
                    public Object setup_BANG_(Object test, Object node)
                    {
                        // Add a callback that will mark the module as having ended _after_ the
                        // callback installed by Module.setup_BANG_ has completed
                        Object result = super.setup_BANG_(test, node);
                        runOnceModule.get().addCompletionCallback(moduleEnded::countDown);
                        return result;
                    }

                    @Override
                    public Object invoke_BANG_(Object test, Object op)
                    {
                        Uninterruptibles.awaitUninterruptibly(moduleEnded);
                        return super.invoke_BANG_(test, op);
                    }

                    @Override
                    public void run(Ensemble ensemble, PropertyGroup properties)
                    {
                        emit(Operation.Type.invoke);
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                        emit(Operation.Type.ok);
                    }
                }
                ));

        final ActiveTestRun activeTestRun = activeTestRunBuilder
            .withTestDefinitionFromYaml(yaml)
            .build();

        final CompletableFuture<TestResult> result = CompletableFuture
            .supplyAsync(() -> performTestRun(activeTestRun));

        assertThatCode(() -> result.get(10, TimeUnit.SECONDS))
            .doesNotThrowAnyException();

        assertThat(moduleEnded.getCount()).isEqualTo(0);

        assertThat(result.join()).isValid();
    }

}
