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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dropwizard.cli.Cli;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.ActiveTestRunBuilder;
import com.datastax.fallout.harness.ClojureShutdown;
import com.datastax.fallout.harness.TestRunAbortedStatusUpdater;
import com.datastax.fallout.ops.JobFileLoggers;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory;
import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.runner.AtomicallyPersistedTestRun;
import com.datastax.fallout.runner.TestRunStateStorage;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.JacksonUtils;
import com.datastax.fallout.util.JsonUtils;
import com.datastax.fallout.util.YamlUtils;

import static com.datastax.fallout.util.YamlUtils.loadYaml;
import static com.google.common.io.MoreFiles.getNameWithoutExtension;

public class FalloutExecCommand<FC extends FalloutConfiguration> extends FalloutCommand<FC>
{
    public static final String CREDS_YAML_FILE_ARG = "creds-yaml-file";
    public static final String TEST_YAML_FILE_ARG = "test-yaml-file";
    public static final String OUTPUT_DIR_ARG = "output-dir";
    private static final String TEMPLATE_PARAMS_YAML_FILE_ARG = "template-params-yaml-file";
    private static final String TEMPLATE_PARAMS_ARGS = "template-params";
    public static final String TESTRUN_JSON = "testrun.json";
    private static final String USE_UNIQUE_OUTPUT_DIR = "--use-unique-output-dir";

    private static class UserError extends RuntimeException
    {
        public UserError(String message, Object... args)
        {
            super(String.format(message, args));
        }
    }

    private static class TestRunFailedError extends RuntimeException
    {
    }

    public FalloutExecCommand(FalloutServiceBase<FC> application)
    {
        super(application, "exec",
            "Run a single testrun in a standalone fallout process and exit");
    }

    @Override
    public void configure(Subparser subparser)
    {
        subparser.addArgument("--params")
            .help("Template parameters YAML file")
            .dest(TEMPLATE_PARAMS_YAML_FILE_ARG)
            .type(Arguments.fileType().verifyExists());

        subparser.addArgument(TEST_YAML_FILE_ARG)
            .help("Test definition YAML file")
            .dest(TEST_YAML_FILE_ARG)
            .type(Arguments.fileType().acceptSystemIn().verifyIsFile().verifyExists());

        subparser.addArgument(CREDS_YAML_FILE_ARG)
            .help("Credentials YAML file")
            .dest(CREDS_YAML_FILE_ARG)
            .type(Arguments.fileType().acceptSystemIn().verifyIsFile().verifyExists());

        subparser.addArgument(OUTPUT_DIR_ARG)
            .help("Where to write testrun artifacts; it's an error if the directory isn't empty")
            .dest(OUTPUT_DIR_ARG)
            .type(Arguments.fileType());

        subparser.addArgument(USE_UNIQUE_OUTPUT_DIR)
            .help(String.format("Write testrun artifacts to %s/TEST_NAME/TESTRUN_ID " +
                "instead of directly into %s", OUTPUT_DIR_ARG, OUTPUT_DIR_ARG))
            .dest(USE_UNIQUE_OUTPUT_DIR)
            .action(Arguments.storeTrue());

        subparser.addArgument(TEMPLATE_PARAMS_ARGS)
            .help(String.format("Template params for %s in the form param=value", TEST_YAML_FILE_ARG))
            .dest(TEMPLATE_PARAMS_ARGS)
            .nargs("*");

        super.configure(subparser);
    }

    /** Make the application configuration YAML into an option rather than an argument */
    @Override
    protected Argument addFileArgument(Subparser subparser)
    {
        return subparser.addArgument("--config")
            .help("Application configuration file")
            // We _must_ call this `file`, because io.dropwizard.cli.ConfiguredCommand#run refers
            // to it by this name
            .dest("file")
            .type(Arguments.fileType().verifyExists());
    }

    @Override
    protected void run(Bootstrap<FC> bootstrap, Namespace namespace,
        FC configuration)
    {
        run(bootstrap.getValidatorFactory().getValidator(),
            configuration,
            namespace.<File>get(TEST_YAML_FILE_ARG).toPath(),
            templateParams(
                Optional.ofNullable(namespace.<File>get(TEMPLATE_PARAMS_YAML_FILE_ARG)).map(File::toPath),
                namespace.get(TEMPLATE_PARAMS_ARGS)),
            namespace.<File>get(CREDS_YAML_FILE_ARG).toPath(),
            namespace.getBoolean(USE_UNIQUE_OUTPUT_DIR),
            namespace.<File>get(OUTPUT_DIR_ARG).toPath());
    }

    private Map<String, Object> templateParams(Optional<Path> paramsFile, List<String> paramItems)
    {
        final var params = new HashMap<String, Object>();

        paramsFile.ifPresent(paramsFile_ -> {
            params.putAll(loadYaml(FileUtils.readString(paramsFile_)));
        });

        paramItems.forEach(paramItem -> {
            final var entry = paramItem.split("=", 2);
            if (entry.length != 2)
            {
                throw new UserError("Template parameter %s should be specified as key=value", paramItem);
            }
            params.put(entry[0], YamlUtils.loadYamlWithType(entry[1]));
        });

        return params;
    }

    private String readString(Path maybeStdinPath)
    {
        try
        {
            if (maybeStdinPath.toString().equals("-"))
            {
                return new String(System.in.readAllBytes(), StandardCharsets.UTF_8);
            }
            else
            {
                return Files.readString(maybeStdinPath);
            }
        }
        catch (IOException e)
        {
            throw new UserError("Couldn't read '%s': %s", maybeStdinPath, e.getMessage());
        }
    }

    private UserCredentials parseUserCredentials(Validator validator, Path credsYamlPath)
    {
        final var objectMapper = JacksonUtils.getYamlObjectMapper();

        final User user;

        try
        {
            user = objectMapper.readValue(readString(credsYamlPath), User.class);
        }
        catch (JsonProcessingException e)
        {
            throw new UserError("Couldn't read user credentials from '%s': %s",
                credsYamlPath, e.getMessage());
        }

        final var errors = validator.validate(user);
        if (!errors.isEmpty())
        {
            throw new UserError("User credentials in '%s' are not valid:\n  %s",
                credsYamlPath,
                errors.stream()
                    .map(error -> String.format("%s %s", error.getPropertyPath(), error.getMessage()))
                    .collect(Collectors.joining("\n  ")));
        }

        return new UserCredentials(user, Optional.empty());
    }

    private void writeTestRunJsonAtomically(TestRun testRun, Path scratchDir, Path outputDir)
    {
        FileUtils.writeString(scratchDir.resolve(TESTRUN_JSON), JsonUtils.toJson(testRun));
        Exceptions.runUncheckedIO(() -> Files.move(scratchDir.resolve(TESTRUN_JSON), outputDir.resolve(TESTRUN_JSON),
            StandardCopyOption.ATOMIC_MOVE));
    }

    private void run(Validator validator, FC configuration, Path testYamlPath,
        Map<String, Object> templateParams,
        Path credsYamlPath, Boolean useUniqueOutputDir, Path rootOutputDir)
    {
        final var testYaml = readString(testYamlPath);

        final var userCredentials = parseUserCredentials(validator, credsYamlPath);

        final var test = Test.createTest(userCredentials.owner.getEmail(),
            getNameWithoutExtension(testYamlPath), testYaml);

        final var testRun = test.createTestRun(templateParams);
        testRun.setCreatedAt(new Date());

        final var outputDir = useUniqueOutputDir ?
            rootOutputDir.resolve(Paths.get(testRun.getTestName(), testRun.getTestRunId().toString())) :
            rootOutputDir;

        if (Files.exists(outputDir) && (!Files.isDirectory(outputDir) ||
            Exceptions.getUncheckedIO(() -> Files.list(outputDir).findAny().isPresent())))
        {
            throw new UserError("%s should either not exist, or be an existing empty directory",
                outputDir);
        }

        final var loggers = new JobFileLoggers(outputDir, true, userCredentials);

        final var scratchSpace = new TestRunScratchSpaceFactory(outputDir).createGlobal();

        final var persistedTestRun = new AtomicallyPersistedTestRun(testRun,
            testRun_ -> writeTestRunJsonAtomically(testRun_, scratchSpace.getPath(), outputDir));

        final var stateStorage = new TestRunStateStorage(persistedTestRun, loggers.getShared(),
            () -> Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(outputDir)));

        final var testRunStatusUpdater = new TestRunAbortedStatusUpdater(stateStorage);
        testRunStatusUpdater.setCurrentState(TestRun.State.PREPARING_RUN);

        ActiveTestRun activeTestRun;
        try
        {
            try
            {
                activeTestRun = ActiveTestRunBuilder.create()
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
            catch (Exception e)
            {
                loggers.getShared().error("Exception building the ActiveTestRun", e);
                testRunStatusUpdater.markFailedWithReason(TestRun.State.FAILED);
                testRunStatusUpdater.markInactive(Optional.empty());
                throw e;
            }

            persistedTestRun
                .update(testRun_ -> testRun_.setResourceRequirements(activeTestRun.getResourceRequirements()));

            activeTestRun.run((message, ex) -> {
                System.err.printf("ERROR: %s\n%s", message, ex.getMessage());
            });

            // To return a non-zero exit code, we need to throw an exception; this is handled in
            // io.dropwizard.cli.Cli#run by first passing the thrown exception it to our onError
            // implementation (which is why we use our own TestRunFailedError, so we can ignore it), then
            // passing it to io.dropwizard.Application#onFatalError, which will call System.exit(1).
            if (stateStorage.getCurrentState() != TestRun.State.PASSED)
            {
                throw new TestRunFailedError();
            }
        }
        finally
        {
            ClojureShutdown.shutdown();
        }
    }

    @Override
    public void onError(Cli cli, Namespace namespace, Throwable e)
    {
        if (e instanceof UserError)
        {
            System.err.printf("ERROR: %s", e.getMessage());
        }
        else if (!(e instanceof TestRunFailedError))
        {
            super.onError(cli, namespace, e);
        }
    }
}
