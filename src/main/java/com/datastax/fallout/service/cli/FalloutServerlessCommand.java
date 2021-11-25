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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.dropwizard.cli.Cli;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.datastax.fallout.runner.UserCredentialsFactory;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.JacksonUtils;
import com.datastax.fallout.util.YamlUtils;

import static com.datastax.fallout.util.YamlUtils.loadYaml;

public abstract class FalloutServerlessCommand<FC extends FalloutConfiguration> extends FalloutCommand<FC>
{
    protected static final String CREDS_YAML_FILE_ARG = "creds-yaml-file";
    protected static final String TEST_YAML_FILE_ARG = "test-yaml-file";
    protected static final String TEMPLATE_PARAMS_YAML_FILE_ARG = "template-params-yaml-file";
    protected static final String TEMPLATE_PARAMS_ARGS = "template-params";

    public FalloutServerlessCommand(FalloutServiceBase<FC> application, String name, String description)
    {
        super(application, name, description);
    }

    @Override
    public final void configure(Subparser subparser)
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

        // if a subclass adds other positional arguments this needs to happen before
        // we add the final "open end" positional arguments below for the template params
        applyParserConfiguration(subparser);

        subparser.addArgument(TEMPLATE_PARAMS_ARGS)
            .help(String.format("Template params for %s in the form param=value",
                TEST_YAML_FILE_ARG))
            .dest(TEMPLATE_PARAMS_ARGS)
            .nargs("*");

        super.configure(subparser);
    }

    protected void applyParserConfiguration(Subparser subparser)
    {
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

    protected Map<String, Object> templateParams(Optional<Path> paramsFile, List<String> paramItems)
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

    protected String readString(Path maybeStdinPath)
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

    protected UserCredentialsFactory.UserCredentials parseUserCredentials(Validator validator, Path credsYamlPath)
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

        return new UserCredentialsFactory.UserCredentials(user, Optional.empty());
    }

    @Override
    public void onError(Cli cli, Namespace namespace, Throwable e)
    {
        if (e instanceof UserError)
        {
            System.err.printf("ERROR: %s", e.getMessage());
        }
        else
        {
            super.onError(cli, namespace, e);
        }
    }

    protected static class UserError extends RuntimeException
    {
        public UserError(String message, Object... args)
        {
            super(String.format(message, args));
        }
    }
}
