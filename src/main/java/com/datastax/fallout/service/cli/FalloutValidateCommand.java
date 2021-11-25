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
package com.datastax.fallout.service.cli;

import javax.validation.Validator;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

import com.datastax.fallout.harness.ActiveTestRunBuilder;
import com.datastax.fallout.harness.ClojureShutdown;
import com.datastax.fallout.harness.InMemoryTestRunStateStorage;
import com.datastax.fallout.harness.TestRunAbortedStatusUpdater;
import com.datastax.fallout.ops.JobConsoleLoggers;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;

import static com.google.common.io.MoreFiles.getNameWithoutExtension;

public class FalloutValidateCommand<FC extends FalloutConfiguration> extends FalloutServerlessCommand<FC>
{
    public FalloutValidateCommand(FalloutServiceBase<FC> application)
    {
        super(application, "validate",
            "Validate a single testrun in a standalone fallout process and exit");
    }

    @Override
    protected void run(Bootstrap<FC> bootstrap, Namespace namespace, FC configuration)
    {
        run(bootstrap.getValidatorFactory().getValidator(),
            configuration,
            namespace.<File>get(TEST_YAML_FILE_ARG).toPath(),
            templateParams(
                Optional.ofNullable(namespace.<File>get(TEMPLATE_PARAMS_YAML_FILE_ARG)).map(File::toPath),
                namespace.get(TEMPLATE_PARAMS_ARGS)),
            namespace.<File>get(CREDS_YAML_FILE_ARG).toPath());
    }

    private void run(Validator validator, FC configuration, Path testYamlPath, Map<String, Object> templateParams,
        Path credsYamlPath)
    {
        final var testYaml = readString(testYamlPath);

        final var userCredentials = parseUserCredentials(validator, credsYamlPath);

        final var test = Test.createTest(userCredentials.owner.getEmail(),
            getNameWithoutExtension(testYamlPath), testYaml);

        final var testRun = test.createTestRun(templateParams);
        testRun.setCreatedAt(new Date());

        final var loggers = new JobConsoleLoggers();

        final var outputDir = Exceptions.getUncheckedIO(() -> Files.createTempDirectory("fallout-validation"));

        final var scratchSpace = new TestRunScratchSpaceFactory(outputDir).createGlobal();

        final var stateStorage = new InMemoryTestRunStateStorage(TestRun.State.PREPARING_RUN);

        final var testRunStatusUpdater = new TestRunAbortedStatusUpdater(stateStorage);

        try
        {
            ActiveTestRunBuilder.create()
                .withFalloutConfiguration(configuration)
                .withTestDefinitionFromYaml(testRun.getExpandedDefinition())
                .withUserCredentials(userCredentials)
                .withTestRunArtifactPath(outputDir)
                .withTestRunStatusUpdater(testRunStatusUpdater)
                .withLoggers(loggers)
                .withTestRunScratchSpace(scratchSpace)
                .withTestRunIdentifier(testRun.getTestRunIdentifier())
                .withTestRun(testRun)
                .build();
        }
        finally
        {
            ClojureShutdown.shutdown();
            Exceptions.runUncheckedIO(
                () -> MoreFiles.deleteRecursively(outputDir, RecursiveDeleteOption.ALLOW_INSECURE));
        }
    }
}
