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
package com.datastax.fallout.harness.checkers;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.ClojureApi;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

import static com.datastax.fallout.harness.JepsenApi.PROCESS;
import static com.datastax.fallout.harness.JepsenApi.TIME;
import static com.datastax.fallout.harness.JepsenApi.VALID;

/**
 * Uses Knossos to check linearizability of a history based on register operations
 */
@AutoService(Checker.class)
public class LinearizabilityChecker extends Checker
{
    static final String prefix = "fallout.checkers.linearizability.";

    private static final Keyword REGISTER = Keyword.intern("register");
    private static final String LWTCLIENT_NAME = "lwtclient";
    private static final Keyword LWTPROCESS = Keyword.intern("lwtprocess");

    static final PropertySpec<String> moduleSpec = PropertySpecBuilder.create(prefix)
        .name("module.check")
        .description("Name of the phase name of the module" +
            " whose history should be parsed. If none is passed in, " +
            "we will assume only one instance of lwtclient is in the test, and will check that.")
        .parser(input -> input)
        .build();

    public LinearizabilityChecker()
    {
        super();
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "linearizability";
    }

    @Override
    public String description()
    {
        return "Checks linearizability of register history";
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.of(moduleSpec);
    }

    @Override
    public boolean check(Ensemble ensemble, Collection<Operation> history)
    {
        // Do not initialize these as static fields, because it will incur the penalty of loading clojure when we
        // load this class for documentation purposes.  Clojure will effectively memoize var calls.
        final var checkLinearizability = ClojureApi.var("knossos.competition", "analysis");
        final var casRegister = ClojureApi.var("knossos.model", "cas-register");
        final var generateReport = ClojureApi.var("knossos.linear.report", "render-analysis!");

        Optional<String> moduleToCheck = Optional.ofNullable(moduleSpec.value(this));
        try
        {
            Collection<IPersistentMap> results =
                history.stream()
                    .filter(op -> {
                        Operation.Type type = op.getType();
                        return type.equals(Operation.Type.invoke) || type.equals(Operation.Type.info) ||
                            type.equals(Operation.Type.ok) || type.equals(Operation.Type.fail);
                    })
                    .filter(op -> filterHistoryForModule(op, moduleToCheck))
                    .map(Operation::getJepsenOp)
                    .map(op -> op.assoc(PROCESS, op.valAt(LWTPROCESS)))
                    .collect(Collectors.groupingBy((IPersistentMap op) -> {
                        Object register = op.valAt(REGISTER);
                        return register == null ? "default" : register;
                    }))
                    .values().stream()
                    .peek(aHistory -> aHistory.sort(Comparator.comparing(o -> ((Long) o.valAt(TIME)))))
                    .map(aHistory -> {
                        IPersistentMap analysis =
                            (IPersistentMap) checkLinearizability.invoke(casRegister.invoke(null), aHistory);
                        if (!(Boolean) analysis.valAt(VALID))
                        {
                            Object register = aHistory.get(0).valAt(REGISTER);
                            logger.info("Analysis failed for the history of register {}.", register);
                            generateReport.invoke(aHistory, analysis,
                                ensemble.getControllerGroup().getNodes().get(0).getRemoteArtifactPath() +
                                    "/nonlinearizable-" + register + ".svg");
                        }
                        return analysis;
                    })
                    .collect(Collectors.toList());

            logger.info("Complete Knossos analysis {}", results);

            return results.stream().allMatch(aResult -> (Boolean) aResult.valAt(VALID));
        }
        catch (Throwable e)
        {
            logger.error("Hit exception while knossos was parsing history: {}", e);
            return false;
        }
    }

    private boolean filterHistoryForModule(Operation op, Optional<String> moduleToCheck)
    {
        if (moduleToCheck.isPresent())
        {
            return op.getModule().getInstanceName().equals(moduleToCheck.get());
        }
        else
        {
            return op.getModule().toString().equals(LWTCLIENT_NAME);
        }
    }
}
