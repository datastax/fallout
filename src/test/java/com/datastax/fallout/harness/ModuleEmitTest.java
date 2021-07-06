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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import com.datastax.fallout.components.impl.FakeModule;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.util.component_discovery.MockingComponentFactory;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.harness.Module.RunToEndOfPhaseMethod.AUTOMATIC;

@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ModuleEmitTest extends EnsembleFalloutTest<FalloutConfiguration>
{
    private final int EMITTERS = 3;
    private final int EMISSIONS = 200000;

    private String emission(int emitter, int emission)
    {
        return String.format("emission %d:%d", emitter, emission);
    }

    /** We don't want to log every emit as that would be excessive, so suppress all logging coming from this module */
    private static class FakeModuleWithSuppressedLogging extends FakeModule
    {
        public FakeModuleWithSuppressedLogging()
        {
            super();
        }

        public FakeModuleWithSuppressedLogging(RunToEndOfPhaseMethod runToEndOfPhaseMethod)
        {
            super(runToEndOfPhaseMethod);
        }

        @Override
        protected Logger logger()
        {
            return NOPLogger.NOP_LOGGER;
        }
    }

    private void runEmitterModuleWithConcurrentModule(Supplier<Module> moduleSupplier)
    {
        String yaml = readYamlFile("module-emit.yaml");

        final ActiveTestRunBuilder activeTestRunBuilder = createActiveTestRunBuilder()
            .withComponentFactory(new MockingComponentFactory()
                .mockNamed(Module.class, "emitter-fake", () -> new FakeModuleWithSuppressedLogging() {
                    @Override
                    public void run(Ensemble ensemble, PropertyGroup properties)
                    {
                        List<CompletableFuture<Void>> futures = IntStream.range(0, EMITTERS)
                            .mapToObj(emitter -> CompletableFuture.runAsync(() -> {
                                IntStream.range(0, EMISSIONS)
                                    .forEach(emission -> emitInfo(emission(emitter, emission)));
                            })).collect(Collectors.toList());
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {})).join();
                    }
                })
                .mockNamed(Module.class, "concurrent-fake", moduleSupplier));

        final ActiveTestRun activeTestRun = activeTestRunBuilder
            .withTestDefinitionFromYaml(yaml)
            .build();

        final Set<String> expectedEmissions = IntStream.range(0, EMITTERS)
            .mapToObj(emitter -> IntStream.range(0, EMISSIONS)
                .mapToObj(emission -> emission(emitter, emission)))
            .flatMap(Function.identity())
            .collect(Collectors.toSet());

        assertThat(
            performTestRun(activeTestRun).history().stream()
                .filter(op -> op.getType() == Operation.Type.info)
                .map(op -> (String) op.getValue())
                .filter(message -> message.startsWith("emission"))
                .collect(Collectors.toSet()))
                    .isEqualTo(expectedEmissions);
    }

    /** This test validates the solution to FAL-1092 */
    @Test
    public void multiple_threads_can_emit_simultaneously_with_run_to_end_of_phase_module()
    {
        runEmitterModuleWithConcurrentModule(() -> new FakeModuleWithSuppressedLogging(AUTOMATIC));
    }

    @Test
    public void multiple_threads_can_emit_simultaneously_with_run_once_module()
    {
        runEmitterModuleWithConcurrentModule(FakeModuleWithSuppressedLogging::new);
    }
}
