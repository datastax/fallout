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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.Workload;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.JobConsoleLoggers;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.ops.configmanagement.FakeConfigurationManager;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;
import com.datastax.fallout.util.Exceptions;

import static org.assertj.core.api.Assertions.assertThat;

public class RegexArtifactCheckerTest extends EnsembleFalloutTest
{
    private static final String ARTIFACT_PATTERN = ".*file\\.txt";
    private static final List<String> REQUIRED_PATTERN = List.of("north");
    private static final List<String> FORBIDDEN_PATTERN = List.of(".*south.*");
    private static final String LINE_MATCHING_REQUIRED = "There are no penguins in the north pole.\n";
    private static final String LINE_MATCHING_FORBIDDEN = "However, there are penguins in the south pole.\n";

    private static Ensemble ensemble;
    private static Path artifactRoot;

    private RegexArtifactChecker regexArtifactChecker;

    @Before
    public void setupEnsembleAndArtifactRoot()
    {
        NodeGroupBuilder nodeGroupBuilder = NodeGroupBuilder.create()
            .withName("server")
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(new FakeConfigurationManager())
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(2)
            .withLoggers(new JobConsoleLoggers())
            .withTestRunArtifactPath(persistentTestOutputDir());

        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withServerGroup(nodeGroupBuilder)
            .withClientGroup(nodeGroupBuilder);

        ensemble = createActiveTestRunBuilder()
            .withEnsembleBuilder(ensembleBuilder, true)
            .withWorkload(new Workload(List.of(), Map.of(), Map.of()))
            .buildEnsemble();

        artifactRoot = persistentTestOutputDir().resolve("artifacts");
    }

    private enum Check
    {
        REQUIRED, FORBIDDEN
    }

    private Set<Check> checksSet(Check... checks)
    {
        return Set.of(checks);
    }

    private void givenChecksFor(Check... checks)
    {
        regexArtifactChecker = new RegexArtifactChecker();
        WritablePropertyGroup properties = new WritablePropertyGroup();
        properties.with(regexArtifactChecker.prefix());
        properties.put("node_group", "server");
        properties.put("file_regex", ARTIFACT_PATTERN);

        if (checksSet(checks).contains(Check.REQUIRED))
        {
            properties.put("required", REQUIRED_PATTERN);
        }
        if (checksSet(checks).contains(Check.FORBIDDEN))
        {
            properties.put("forbidden", FORBIDDEN_PATTERN);
        }
        regexArtifactChecker.setProperties(properties);
    }

    private void whenArtifactsContainMatchesFor(Check... checks)
    {
        Path serverArtifacts = artifactRoot.resolve("server");

        Path nodeGroupArtifact = serverArtifacts.resolve("server-file.txt");
        createArtifact(nodeGroupArtifact, checks);

        Path node0Artifact = serverArtifacts.resolve("node0/node0-file.txt");
        createArtifact(node0Artifact, checks);

        Path node1Artifact = serverArtifacts.resolve("node1/node1-file.txt");
        createArtifact(node1Artifact, checks);
    }

    private void createArtifact(Path artifactPath, Check... checks)
    {
        boolean artifactReadyToWrite;
        if (artifactPath.toFile().exists())
        {
            artifactReadyToWrite = artifactPath.toFile().delete();
        }
        else
        {
            File parent = artifactPath.getParent().toFile();
            artifactReadyToWrite = parent.exists() || parent.mkdirs();
        }

        if (!artifactReadyToWrite)
        {
            throw new RuntimeException(String.format("Something went wrong prepping %s", artifactPath));
        }

        StringBuilder fileContent = new StringBuilder();
        if (checksSet(checks).contains(Check.REQUIRED))
        {
            fileContent.append(LINE_MATCHING_REQUIRED);
        }
        if (checksSet(checks).contains(Check.FORBIDDEN))
        {
            fileContent.append(LINE_MATCHING_FORBIDDEN);
        }

        Exceptions.runUnchecked(() -> {
            Files.createFile(artifactPath);
            Utils.writeStringToFile(artifactPath.toFile(), fileContent.toString());
        });
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
        return regexArtifactChecker.validate(ensemble, artifactRoot);
    }

    @Test
    public void validate_fails_when_forbidden_pattern_is_found()
    {
        givenChecksFor(Check.REQUIRED, Check.FORBIDDEN);
        whenArtifactsContainMatchesFor(Check.REQUIRED, Check.FORBIDDEN);
        thenValidateFails();

        givenChecksFor(Check.REQUIRED, Check.FORBIDDEN);
        whenArtifactsContainMatchesFor(Check.FORBIDDEN);
        thenValidateFails();

        givenChecksFor(Check.FORBIDDEN);
        whenArtifactsContainMatchesFor(Check.REQUIRED, Check.FORBIDDEN);
        thenValidateFails();

        givenChecksFor(Check.FORBIDDEN);
        whenArtifactsContainMatchesFor(Check.FORBIDDEN);
        thenValidateFails();
    }

    @Test
    public void validate_passes_when_forbidden_pattern_is_not_found()
    {
        givenChecksFor(Check.FORBIDDEN);
        whenArtifactsContainMatchesFor();
        thenValidatePasses();
    }

    @Test
    public void validate_passes_when_required_pattern_is_found()
    {
        givenChecksFor(Check.REQUIRED);
        whenArtifactsContainMatchesFor(Check.REQUIRED);
        thenValidatePasses();

        givenChecksFor(Check.REQUIRED);
        whenArtifactsContainMatchesFor(Check.REQUIRED, Check.FORBIDDEN);
        thenValidatePasses();
    }

    @Test
    public void validate_fails_when_required_pattern_is_not_found()
    {
        givenChecksFor(Check.REQUIRED);
        whenArtifactsContainMatchesFor();
        thenValidateFails();
    }
}
