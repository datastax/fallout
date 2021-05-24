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

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.Workload;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.components.file_artifact_checkers.RegexArtifactCheckerTest.Check.FORBIDDEN;
import static com.datastax.fallout.components.file_artifact_checkers.RegexArtifactCheckerTest.Check.REQUIRED;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class RegexArtifactCheckerTest extends EnsembleFalloutTest<FalloutConfiguration>
{
    private static final String ARTIFACT_PATTERN = ".*file\\.txt";

    private static Ensemble ensemble;
    private static Path artifactRoot;

    private RegexArtifactChecker regexArtifactChecker;

    @BeforeEach
    public void setupEnsembleAndArtifactRoot()
    {
        NodeGroupBuilder nodeGroupBuilder = NodeGroupBuilder.create()
            .withName("server")
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(new FakeConfigurationManager())
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(2)
            .withTestRunArtifactPath(testRunArtifactPath());

        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withServerGroup(nodeGroupBuilder)
            .withClientGroup(nodeGroupBuilder);

        ensemble = createActiveTestRunBuilder()
            .withEnsembleBuilder(ensembleBuilder)
            .withWorkload(new Workload(List.of(), new HashMap<>(), new HashMap<>()))
            .build()
            .getEnsemble();

        artifactRoot = testRunArtifactPath();
    }

    enum Check
    {
        REQUIRED(
            "north",
            "There are no penguins at the north pole."),
        FORBIDDEN(
            ".*south.*",
            "However, there are penguins at the south pole.");

        public final String propertyName;
        public final String pattern;
        public final String artifactContentsMatchingPattern;

        Check(String pattern, String line)
        {
            this.propertyName = name().toLowerCase();
            this.pattern = pattern;
            this.artifactContentsMatchingPattern = line;
        }
    }

    private void whenCheckingFor(String fileRegex, Check... checks)
    {
        regexArtifactChecker = new RegexArtifactChecker();
        WritablePropertyGroup properties = new WritablePropertyGroup();
        properties.with(regexArtifactChecker.prefix())
            .put("node_group", "server")
            .put("files", fileRegex);

        for (final var check : checks)
        {
            properties.put(check.propertyName, List.of(check.pattern));
        }

        regexArtifactChecker.setProperties(properties);
    }

    private void whenCheckingFor(Check... checks)
    {
        whenCheckingFor(ARTIFACT_PATTERN, checks);
    }

    private void givenArtifactsContainingMatchesFor(Check... checks)
    {
        givenArtifactContainingMatchesFor("server/server-file.txt", checks);
        givenArtifactContainingMatchesFor("server/node0/node0-file.txt", checks);
        givenArtifactContainingMatchesFor("server/node1/node1-file.txt", checks);
    }

    private void givenArtifactContainingMatchesFor(String relativePath, Check... checks)
    {
        TestHelpers.createArtifact(testRunArtifactPath(), relativePath,
            Arrays.stream(checks)
                .map(check -> check.artifactContentsMatchingPattern)
                .collect(Collectors.joining("\n", "", "\n")));
    }

    private void thenValidatePasses()
    {
        assertThat(validate()).isTrue();
    }

    private void thenValidateFails()
    {
        assertThat(validate()).isFalse();
    }

    private boolean validate()
    {
        return regexArtifactChecker.checkArtifacts(ensemble, artifactRoot);
    }

    private static Check[] checks(Check... checks_)
    {
        return checks_;
    }

    private static Stream<Arguments> validate_fails_when_forbidden_pattern_is_specified_and_found_params()
    {
        return Stream.of(
            arguments(
                checks(REQUIRED, FORBIDDEN),
                checks(REQUIRED, FORBIDDEN)),
            arguments(
                checks(REQUIRED, FORBIDDEN),
                checks(FORBIDDEN)),
            arguments(
                checks(FORBIDDEN),
                checks(REQUIRED, FORBIDDEN)),
            arguments(
                checks(FORBIDDEN),
                checks(FORBIDDEN)));
    }

    @ParameterizedTest
    @MethodSource("validate_fails_when_forbidden_pattern_is_specified_and_found_params")
    public void validate_fails_when_forbidden_pattern_is_specified_and_found(
        Check[] checksSpecified, Check[] artifactsContain)
    {
        givenArtifactsContainingMatchesFor(artifactsContain);
        whenCheckingFor(checksSpecified);
        thenValidateFails();
    }

    @Test
    public void validate_passes_when_forbidden_pattern_is_not_found()
    {
        givenArtifactsContainingMatchesFor();
        whenCheckingFor(FORBIDDEN);
        thenValidatePasses();
    }

    private static Stream<Arguments> validate_passes_when_required_pattern_is_specified_and_found_params()
    {
        return Stream.of(
            arguments(
                checks(REQUIRED),
                checks(REQUIRED)),
            arguments(
                checks(REQUIRED),
                checks(REQUIRED, FORBIDDEN)));
    }

    @ParameterizedTest
    @MethodSource("validate_passes_when_required_pattern_is_specified_and_found_params")
    public void validate_passes_when_required_pattern_is_specified_and_found(
        Check[] checksSpecified, Check[] artifactsContain)
    {
        givenArtifactsContainingMatchesFor(artifactsContain);
        whenCheckingFor(checksSpecified);
        thenValidatePasses();
    }

    @Test
    public void validate_fails_when_required_pattern_is_not_found()
    {
        givenArtifactsContainingMatchesFor();
        whenCheckingFor(REQUIRED);
        thenValidateFails();
    }

    @Test
    public void filters_using_full_paths()
    {
        givenArtifactContainingMatchesFor("server/node0/cndb/stuff.log", FORBIDDEN);
        givenArtifactContainingMatchesFor("server/node1/cndb/stuff.log");

        whenCheckingFor(".*/cndb/.*\\.log", FORBIDDEN);
        thenValidateFails();

        whenCheckingFor(".*node1/cndb/.*\\.log", FORBIDDEN);
        thenValidatePasses();
    }
}
