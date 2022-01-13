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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import clojure.lang.APersistentMap;
import clojure.lang.Keyword;

import static com.datastax.fallout.harness.JepsenApi.VALID;

/**
 * Wrapper class for results return by a Jepsen test
 */
public class TestResult
{
    private final Map jepsenTestMap;

    private boolean artifactCheckersValid = true;

    TestResult(Map jepsenTestMap)
    {
        this.jepsenTestMap = jepsenTestMap;
    }

    /**
     * Whether the test is valid
     * @return true the :valid? entry in the jepsenTestMap
     */
    public boolean isValid()
    {
        return Optional.ofNullable((Boolean) results().get(VALID))
            .orElse(Boolean.FALSE) && artifactCheckersValid;
    }

    public void setArtifactCheckersValid(boolean artifactCheckersValid)
    {
        this.artifactCheckersValid = artifactCheckersValid;
    }

    /**
     * Verbose, unstructured results data returned from Jepsen
     * @return the unwrapped :results entry in the jepsenTestMap
     */
    public Map results()
    {
        return Optional.ofNullable((Map) jepsenTestMap.get(Keyword.intern("results")))
            .orElse(Map.of());
    }

    public List<Operation> history()
    {
        return Optional.ofNullable(((Collection<APersistentMap>) jepsenTestMap.get(Keyword.intern("history"))).stream()
            .map(Operation::fromOpMap)
            .toList())
            .orElse(List.of());
    }
}
