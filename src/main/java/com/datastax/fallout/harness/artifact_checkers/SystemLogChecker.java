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
package com.datastax.fallout.harness.artifact_checkers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.LocalScratchSpace;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.utils.FileUtils;

/**
 * Checks the archived system.log for the given error pattern.
 */
@AutoService(ArtifactChecker.class)
public class SystemLogChecker extends ArtifactChecker
{

    private static final String prefix = "fallout.artifact_checkers.systemlog.";
    private static final PropertySpec<String> serverGroupSpec = PropertySpecBuilder.serverGroup(prefix);
    private static final PropertySpec<String> errorPatternSpec = PropertySpecBuilder.create(prefix)
        .name("error.pattern")
        .description("The regex error pattern to check for. Note that this pattern must match an entire line.")
        .defaultOf("^(ERROR|FATAL).*")
        .parser(input -> input)
        .validator(p -> {
            try
            {
                Pattern.compile((String) p);
                return true;
            }
            catch (PatternSyntaxException | ClassCastException e)
            {
                return false;
            }
        }).build();

    /**
     * @return the prefix for properties of this Component
     */
    @Override
    public String prefix()
    {
        return prefix;
    }

    /**
     * @return a unique name for this Component
     */
    @Override
    public String name()
    {
        return "systemlog";
    }

    /**
     * @return the long description of this Component
     */
    @Override
    public String description()
    {
        return "Checks the system.log for the given error pattern.";
    }

    /**
     * @return the list of properties this Component will accept
     */
    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(serverGroupSpec, errorPatternSpec)
            .build();
    }

    @Override
    public boolean validate(Ensemble ensemble, Path rootArtifactLocation)
    {
        try (LocalScratchSpace localScratchSpace = new LocalScratchSpace("artifactCheckers"))
        {
            Path scratchDir = localScratchSpace.createDirectory("sys-log-checker");
            NodeGroup serverGroup = ensemble.getServerGroup(serverGroupSpec, getProperties());
            List<Boolean> foundErrors = serverGroup.getNodes().stream()
                .flatMap(
                    node -> FileUtils.getSortedLogList(node.getLocalArtifactPath(), "system.log", logger, scratchDir)
                        .stream()
                        .map(log -> foundErrorInSystemLog(log, node.getId())))
                .collect(Collectors.toList());
            // true if no error was found, otherwise false
            return foundErrors.stream().noneMatch(res -> res);
        }
    }

    private boolean foundErrorInSystemLog(Path systemLog, String nodeName)
    {
        AtomicBoolean hasLines = new AtomicBoolean(false);
        Pattern errorPattern = Pattern.compile(errorPatternSpec.value(this));
        Predicate<String> matchesPattern = line -> {
            hasLines.set(true);
            return errorPattern.matcher(line).matches();
        };

        try (Stream<String> lines = Files.lines(systemLog))
        {
            List<String> foundErrorLines = lines.filter(matchesPattern)
                .limit(5)
                .collect(Collectors.toList());
            if (!hasLines.get())
            {
                logger.error("System.log of {} is empty", nodeName);
                return true;
            }
            String msg = "{} errors in system.log of {} using pattern '{}'";
            if (foundErrorLines.isEmpty())
            {
                logger.info(msg, "No", nodeName, errorPattern.pattern());
                return false;
            }
            else
            {
                logger.error(msg, "Found", nodeName, errorPattern.pattern());
                logger.error("First offending lines from system.log:\n{}", String.join("\n", foundErrorLines));
                return true;
            }
        }
        catch (IOException e)
        {
            logger.warn(
                String.format("system.log of %s could not be found under %s. Error was: %s", nodeName, systemLog, e));
        }
        return true;
    }
}
