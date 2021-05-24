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
package com.datastax.fallout.components.fallout_system;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.collect.EvictingQueue;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.service.core.TestRun;

@AutoService(ArtifactChecker.class)
public class SmartLogChecker extends ArtifactChecker
{
    private static final String prefix = "fallout.artifact_checkers.smartlog.";
    private final Optional<TestRun> testRun;

    private static final PropertySpec<Integer> snippetSizeSpec = PropertySpecBuilder.createInt(prefix)
        .name("snippet.length")
        .description("The number of lines to capture")
        .defaultOf(50)
        .build();

    /**
     * This constructor is needed for the ServiceLoader. When instantiating directly, the testRun should be passed in.
     */
    public SmartLogChecker()
    {
        testRun = Optional.empty();
    }

    public SmartLogChecker(TestRun testRun)
    {
        this.testRun = Optional.of(testRun);
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "smartlogs";
    }

    @Override
    public String description()
    {
        return "Checks for fallout-errors.log and the fallout-shared.log for important error messages, and surfaces those for easier user parsing.";
    }

    @Override
    public boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        if (!testRun.isPresent())
        {
            logger().warn("SmartLogChecker has no testRun to send results to; skipping");
            return true;
        }
        TestRun targetTestRun = testRun.get();
        int snippetSize = snippetSizeSpec.value(this);
        try
        {
            Path artifactsRoot = rootArtifactLocation.toAbsolutePath();
            Path errorsLogPath = artifactsRoot.resolve("fallout-errors.log");

            List<String> logLines = List.of();
            if (errorsLogPath.toFile().exists())
            {
                // for the errors log we take the first N lines of the file
                try (Stream<String> lines = Files.lines(errorsLogPath).limit(snippetSize))
                {
                    logLines = lines.collect(Collectors.toList());
                }
            }
            if (logLines.isEmpty())
            {
                // this takes the N lines before the first error in the shared log
                logLines = parseLog(artifactsRoot.resolve("fallout-shared.log"), snippetSize);
            }
            targetTestRun.setParsedLogInfo(String.join("\n", logLines));
        }
        catch (IOException e)
        {
            logger().error("Error parsing logs", e);
            return false;
        }
        return true;
    }

    private static List<String> parseLog(Path log, int snippetSize) throws IOException
    {
        Queue<String> snippetLines = EvictingQueue.create(snippetSize);
        try (Stream<String> lines = Files.lines(log))
        {
            for (String logLine : (Iterable<String>) lines::iterator)
            {
                snippetLines.add(logLine);
                if (logLine.contains("FAILED"))
                {
                    return new ArrayList<>(snippetLines);
                }
            }
        }
        return List.of();
    }
}
