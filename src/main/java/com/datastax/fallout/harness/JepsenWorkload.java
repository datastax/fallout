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
package com.datastax.fallout.harness;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import clojure.java.api.Clojure;
import clojure.lang.APersistentMap;
import clojure.lang.Keyword;
import clojure.lang.PersistentHashMap;
import clojure.tools.logging.impl.LoggerFactory;
import jepsen.generator.Generator;
import org.slf4j.Logger;

import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.harness.ClojureApi.*;
import static com.datastax.fallout.harness.JepsenApi.*;

/** Responsible for mapping a {@link Workload} into the clojure structures used by Jepsen, and then running it */
public class JepsenWorkload
{
    private final Logger logger;
    private final APersistentMap start;
    private final TestRunAbortedStatusUpdater testRunStatusUpdater;
    private final Workload workload;
    private final Ensemble ensemble;

    private JepsenWorkload(Logger logger,
        TestRunAbortedStatusUpdater testRunStatusUpdater, Workload workload, Ensemble ensemble)
    {
        this.logger = logger;
        this.testRunStatusUpdater = testRunStatusUpdater;
        this.workload = workload;
        this.ensemble = ensemble;
        start = (APersistentMap) Clojure.read("{:type :start}");
    }

    /** Runs a test inside Jepsen */
    static Optional<TestResult> run(Logger logger, Ensemble ensemble, Workload workload,
        TestRunAbortedStatusUpdater testRunStatusUpdater)
    {
        return new JepsenWorkload(logger, testRunStatusUpdater, workload, ensemble).run();
    }

    public Optional<TestResult> run()
    {
        Optional<TestResult> testResult = Optional.empty();

        // Base test object which we layer our ensemble/workload specific information on top of. Should be
        // refactored to a constant somewhere.
        Object baseTest = assoc.invoke(
            dissoc.invoke(deref.invoke(noop), NODES, NAME),
            CONCURRENCY, 1);

        Object testWithEnsemble = assoc.invoke(
            baseTest,
            ENSEMBLE, ensemble);

        Map<String, Checker> checkers = workload.getCheckers();

        Map<Object, Object> jepsenWorkload = new HashMap<Object, Object>()
        {
            {
                put(CONDUCTORS, conductors(workload.getPhases()));
                put(CHECKER, checkerCompose.invoke(checkers));
                put(GENERATOR, generator(workload.getPhases()));
            }
        };

        Object readyToRunTest = merge.invoke(testWithEnsemble, jepsenWorkload);

        testRunStatusUpdater.setCurrentState(TestRun.State.RUNNING);
        logger.info("Starting Jepsen test");

        final LoggerFactory jepsenLoggerFactory = new LoggerFactory()
        {
            @Override
            public Object name()
            {
                return "fallout-jepsen";
            }

            @Override
            public Object get_logger(Object o)
            {
                return logger;
            }
        };

        Map jepsenResults = (Map) withBindings.invoke(
            PersistentHashMap.create(loggerFactory, jepsenLoggerFactory),
            JepsenApi.run, readyToRunTest);

        if (jepsenResults == null)
        {
            logger.error("Jepsen returned no result, check logs");
            testRunStatusUpdater.markFailedWithReason(TestRun.State.FAILED);
        }
        else
        {
            testResult = Optional.of(new TestResult(jepsenResults));
        }

        logger.info("Jepsen test finished");

        return testResult;
    }

    private static Set<Keyword> modules(Collection<Phase> phases)
    {
        Set<Keyword> modules = new HashSet<>();
        for (Phase phase : phases)
        {
            phase.getAllModulesRecursively().keySet().stream().map(m -> Keyword.intern(m)).forEach(modules::add);
        }

        return modules;
    }

    private Generator subgenerator(List<Phase> phases)
    {
        List<Generator> generatorPhases = new ArrayList<>();
        Set<Keyword> modules = modules(phases);

        logger.info("Phases {}", phases);

        for (Phase phase : phases)
        {
            Map<String, Module> topLevelModules = phase.getTopLevelModules();
            Map<String, List<Phase>> subPhases = phase.getSubPhases();

            String phaseName = phase.name;
            generatorPhases.add(
                (Generator) apply.invoke(concatGen,
                    onceGen.invoke(
                        (Generator) (test, process) -> {
                            logger.info("Starting phase {}", phaseName);
                            return null;
                        }),
                    topLevelModules.entrySet().stream()
                        .map(Map.Entry::getValue)
                        .reduce(
                            voidGen,
                            (gen, module) -> (Generator) conductorGen.invoke(
                                Keyword.intern(module.getInstanceName()),
                                onceGen.invoke(start.assoc(MODULE, module)),
                                gen),
                            (gen1, gen2) -> null
                        ),
                    subPhases.values().stream()
                        .map(this::subgenerator)
                        .collect(Collectors.toList()))
            );
        }

        return (Generator) onGen.invoke(into.invoke(Clojure.read("#{}"), modules),
            apply.invoke(phasesGen, generatorPhases));
    }

    private Generator generator(List<Phase> phases)
    {
        Generator baseGen = subgenerator(phases);

        /* Jepsen starts running checkers after all the client threads are done;
        since we are running our modules as conductors and not clients, we need
        a final void generator for the client to block on while the last phase of
        modules runs.
         */

        return (Generator) phasesGen.invoke(baseGen, voidGen);
    }

    private void conductors(List<Phase> phases, Map<Object, Object> conductorsMap)
    {
        for (Phase phase : phases)
        {
            for (Map.Entry<String, Module> moduleEntry : phase.getTopLevelModules().entrySet())
            {
                Module module = moduleEntry.getValue();

                if (module.runsToEndOfPhase())
                {
                    phase.getAllModulesRecursively().values().stream().forEach((Module aMod) -> {
                        if (aMod != module && !(aMod.runsToEndOfPhase()))
                        {
                            module.addRunOnceModule(aMod);
                        }
                    });
                }

                conductorsMap.put(Keyword.intern(moduleEntry.getKey()),
                    new AbortableModule(testRunStatusUpdater, module));
            }

            for (List<Phase> phaseList : phase.getSubPhases().values())
            {
                conductors(phaseList, conductorsMap);
            }
        }
    }

    private Map<Object, Object> conductors(List<Phase> phases)
    {
        Map<Object, Object> conductors = new HashMap<>();

        conductors(phases, conductors);

        return conductors;
    }
}
