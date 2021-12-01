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
package com.datastax.fallout.components.file_artifact_checkers;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

@AutoService(ArtifactChecker.class)
public class RegexArtifactChecker extends ArtifactChecker
{
    private static final String prefix = "fallout.artifact_checkers.regex.";

    private static final PropertySpec<String> artifactSourceGroupSpec = PropertySpecBuilder.nodeGroup(prefix);

    private static final PropertySpec<Pattern> fileRegex = PropertySpecBuilder.createRegex(prefix)
        .name("files")
        .description("Check all artifacts in the node_group artifacts path whose path completely match this regex")
        .required()
        .build();

    private static final PropertySpec<List<Pattern>> requiredPatternSpec = PropertySpecBuilder.createRegexList(prefix)
        .name("required")
        .description("A list of regexes; all must match for the validation to pass. Each regex will be used to " +
            "check every line in the file. While not required, it is recommended regexes be written to match " +
            "an entire line.")
        .build();

    private static final PropertySpec<List<Pattern>> forbiddenPatternSpec = PropertySpecBuilder.createRegexList(prefix)
        .name("forbidden")
        .description("A list of regexes; none must match for the validation to pass. Each regex will be used to " +
            "check every line in the file. While not required, it is recommended regexes be written to match " +
            "an entire line.")
        .build();

    private static final PropertySpec<Boolean> validateAllSpec = PropertySpecBuilder.createBool(prefix, false)
        .name("validate_all")
        .description("Whether to validate all matched artifacts or stop at first failed artifacts. Default: false")
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "regex";
    }

    @Override
    public String description()
    {
        return "Checks the specified artifact at file_path, or artifacts matching file_regex, against the required " +
            "and forbidden regexes. Validation fails if any artifact does not match ALL the required regexes OR if " +
            "any artifact contains ANY of the forbidden regexes.";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(artifactSourceGroupSpec, fileRegex,
            forbiddenPatternSpec, requiredPatternSpec,
            validateAllSpec);
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        if (forbiddenPatternSpec.optionalValue(properties).map(List::size).orElse(0) == 0 &&
            requiredPatternSpec.optionalValue(properties).map(List::size).orElse(0) == 0)
        {
            throw new PropertySpec.ValidationException("At least one of required or forbidden must be specified");
        }
    }

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        validator.requireNodeGroup(artifactSourceGroupSpec);
    }

    @Override
    public boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        String targetNodeGroupAlias = artifactSourceGroupSpec.value(getProperties());
        Path nodeGroupArtifactsRoot = ensemble.getNodeGroupByAlias(targetNodeGroupAlias).getLocalArtifactPath();
        List<Path> artifacts = findArtifactsRelativeToRoot(rootArtifactLocation, nodeGroupArtifactsRoot);
        if (artifacts.isEmpty())
        {
            if (requiredPatternSpec.value(getProperties()) != null)
            {
                logger().error("No artifacts found, cannot find required patterns. Failing validation.");
                return false;
            }
            if (forbiddenPatternSpec.value(getProperties()) != null)
            {
                logger().info("No artifacts found, cannot find forbidden patterns. Passing validation.");
                return true;
            }
        }

        boolean validateAll = validateAllSpec.value(getProperties());

        boolean passed = true;
        for (Path artifactPath : artifacts)
        {
            passed &= artifactPassesValidation(rootArtifactLocation, artifactPath);
            if (!validateAll && !passed)
                return false;
        }
        return passed;
    }

    private List<Path> findArtifactsRelativeToRoot(Path rootArtifactLocation,
        Path nodeGroupArtifactsRoot)
    {
        final var matches = fileRegex.value(getProperties()).asMatchPredicate();
        try (Stream<Path> pathStream = Files.walk(nodeGroupArtifactsRoot, FileVisitOption.FOLLOW_LINKS))
        {
            return pathStream
                .map(nodeGroupArtifactsRoot::relativize)
                .filter(path -> matches.test(path.toString()))
                .map(path -> rootArtifactLocation.relativize(nodeGroupArtifactsRoot.resolve(path)))
                .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            logger().error("Exception while finding artifacts", e);
            return List.of();
        }
    }

    private boolean artifactPassesValidation(Path rootArtifactPath, Path artifactpath)
    {
        logger().info("Checking '{}'", artifactpath);
        boolean containsRequiredMatches = requiredPatternSpec.optionalValue(getProperties())
            .map(requiredPatterns -> artifactContainsRequiredPatternMatches(rootArtifactPath, artifactpath,
                requiredPatterns))
            .orElse(true);
        boolean containsForbiddenMatches = forbiddenPatternSpec.optionalValue(getProperties())
            .map(forbiddenPatterns -> artifactContainsForbiddenPatternMatches(rootArtifactPath, artifactpath,
                forbiddenPatterns))
            .orElse(false);
        return containsRequiredMatches && !containsForbiddenMatches;
    }

    private boolean artifactContainsRequiredPatternMatches(Path rootArtifactPath, Path artifact,
        List<Pattern> requiredPatterns)
    {
        return artifactContainsPatternMatches(rootArtifactPath, artifact, requiredPatterns, true, (matchingLines) -> {
            if (matchingLines.isEmpty())
            {
                logger().error(String.format("Found zero lines in %s matching any required pattern '%s'", artifact,
                    requiredPatterns));
            }
        });
    }

    private boolean artifactContainsForbiddenPatternMatches(Path rootArtifactPath, Path artifact,
        List<Pattern> forbiddenPatterns)
    {
        return artifactContainsPatternMatches(rootArtifactPath, artifact, forbiddenPatterns, false, (matchingLines) -> {
            if (!matchingLines.isEmpty())
            {
                String matches = matchingLines.entrySet().stream()
                    .map(patternEntry -> String.format("Lines matching %s:\n\t%s", patternEntry.getKey(),
                        String.join("\n\t", patternEntry.getValue())))
                    .collect(Collectors.joining("\n"));
                logger().error(String.format("Found forbidden pattern(s) in '%s'\n%s", artifact, matches));
            }
        });
    }

    private boolean artifactContainsPatternMatches(Path rootArtifactPath, Path artifact, List<Pattern> patterns,
        boolean exceptionResult,
        Consumer<Map<Pattern, List<String>>> matchingLinesHandler)
    {
        try (Stream<String> lines = Files.lines(rootArtifactPath.resolve(artifact)))
        {
            Map<Pattern, List<String>> matchingLinesByPattern = new HashMap<>();
            lines.forEach(line -> patterns.forEach(p -> {
                if (p.matcher(line).find())
                {
                    matchingLinesByPattern.computeIfAbsent(p, ignored -> new ArrayList<>()).add(line);
                }
            })
            );

            matchingLinesHandler.accept(matchingLinesByPattern);
            return !matchingLinesByPattern.isEmpty();
        }
        catch (IOException e)
        {
            logger().error(String.format("Exception while reading %s", artifact), e);
            return exceptionResult;
        }
    }
}
