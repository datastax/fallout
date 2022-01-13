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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.JobLoggers;

/**
 * Along with an Ensemble, a Workload is one of the two components needed to start a test run. In general,
 * an Ensemble contains information about how to provision/configure the machines needed for different parts of a test.
 * A Workload contains information about how to run the test and check results. Phases is used for the phases of modules
 * that run during a test; checkers represent all the checkers to be run after the phases complete.
 *
 * @see ActiveTestRunBuilder
 * @see ActiveTestRun
 */

public class Workload
{
    final private List<Phase> phases;
    final private Map<String, Checker> checkers;
    final private Map<String, ArtifactChecker> artifactCheckers;

    public Workload(List<Phase> phases, Map<String, Checker> checkers, Map<String, ArtifactChecker> artifactCheckers)
    {
        this.phases = phases != null ? phases : new ArrayList<>();
        this.checkers = checkers != null ? checkers : new LinkedHashMap<>();
        this.artifactCheckers = artifactCheckers != null ? artifactCheckers : new LinkedHashMap<>();
    }

    public Map<String, Checker> getCheckersJepsenMap()
    {
        return checkers;
    }

    public Collection<Checker> getCheckers()
    {
        return checkers.values();
    }

    public Collection<ArtifactChecker> getArtifactCheckers()
    {
        return artifactCheckers.values();
    }

    public List<Phase> getPhases()
    {
        return phases;
    }

    public List<Module> getAllModules()
    {
        return getPhases().stream()
            .map(Phase::getAllModulesRecursively)
            .map(Map::values)
            .flatMap(Collection::stream)
            .toList();
    }

    public void addChecker(String name, Checker checker)
    {
        checker.setInstanceName(name);
        Checker old = checkers.put(name, checker);
        if (old != null)
        {
            throw new InvalidConfigurationException("Duplicate Checker name: " + name);
        }
    }

    public void addArtifactChecker(String name, ArtifactChecker artifactChecker)
    {
        artifactChecker.setInstanceName(name);
        ArtifactChecker old = artifactCheckers.put(name, artifactChecker);
        if (old != null)
        {
            throw new InvalidConfigurationException("Duplicate ArtifactChecker name: " + name);
        }
    }

    public void setLoggers(JobLoggers loggers)
    {
        for (Map.Entry<String, Checker> entry : checkers.entrySet())
        {
            Path checkerLog =
                Paths.get(EnsembleBuilder.CONTROLLER_NAME, "checkers", "checker-" + entry.getKey() + ".log");
            entry.getValue().setLogger(loggers.create(entry.getKey(), checkerLog));
        }
        for (Map.Entry<String, ArtifactChecker> entry : artifactCheckers.entrySet())
        {
            Path artifactCheckerLog = Paths.get(EnsembleBuilder.CONTROLLER_NAME, "artifact_checkers",
                "artifact_checker-" + entry.getKey() + ".log");
            entry.getValue().setLogger(loggers.create(entry.getKey(), artifactCheckerLog));
        }
    }
}
