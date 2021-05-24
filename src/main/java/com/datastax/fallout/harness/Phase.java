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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Phase
{
    public final String name;
    private final Map<String, List<Phase>> subPhases;
    private final Map<String, Module> modules;

    public Phase(String name, Map<String, Module> modules)
    {
        this(name, new HashMap<>(), modules);
    }

    public Phase(String name, Map<String, List<Phase>> subPhases, Map<String, Module> modules)
    {
        this.name = name;
        this.subPhases = subPhases;
        this.modules = modules;
    }

    /**
     * Is this phase a leaf in the tree, i.e., does it only contain modules
     * @return true if this only contains modules, false if it contains any phases
     */
    public boolean isLeaf()
    {
        return subPhases != null && subPhases.isEmpty();
    }

    public Map<String, List<Phase>> getSubPhases()
    {
        return subPhases;
    }

    public Map<String, Module> getTopLevelModules()
    {
        return modules;
    }

    public Map<String, Module> getAllModulesRecursively()
    {
        HashMap<String, Module> allModules = new HashMap<>();
        allModules.putAll(modules);
        subPhases.entrySet()
            .stream()
            .map(Map.Entry::getValue)
            .flatMap(Collection::stream)
            .map(Phase::getAllModulesRecursively)
            .forEach(allModules::putAll);
        return allModules;
    }

    public String toString()
    {
        return String.format("Phase %s: Contains modules: [%s] and subphase lists: [%s]",
            name,
            String.join(", ", modules.keySet().stream()
                .collect(Collectors.toList())),
            String.join(", ", subPhases.keySet().stream()
                .collect(Collectors.toList())));
    }
}
