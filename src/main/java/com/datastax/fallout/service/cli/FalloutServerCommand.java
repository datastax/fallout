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
package com.datastax.fallout.service.cli;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import io.dropwizard.cli.ServerCommand;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutConfigurationFactoryFactory;
import com.datastax.fallout.service.FalloutService;

public abstract class FalloutServerCommand extends ServerCommand<FalloutConfiguration>
{
    public static final String PID_FILE_OPTION_NAME = "pidFile";

    protected FalloutServerCommand(FalloutService falloutService, String name, String description)
    {
        super(falloutService, name, description);
    }

    @Override
    public void configure(Subparser subparser)
    {
        subparser.addArgument("--pid-file")
            .help(String.format("Write the process ID to %s (or %s if --runner-id is specified)",
                FalloutConfiguration.getPidFile(Paths.get("FALLOUT_HOME"), Optional.empty()),
                FalloutConfiguration.getPidFile(Paths.get("FALLOUT_HOME"), Optional.of(99))
                    .toString().replace("/99/", "/RUNNER-ID/")))
            .action(Arguments.storeTrue())
            .dest(PID_FILE_OPTION_NAME);

        super.configure(subparser);
    }

    /** We override this overload so that we can install
     *  {@link FalloutConfigurationFactoryFactory} before the configuration is parsed. */
    @Override
    @SuppressWarnings("unchecked")
    public void run(Bootstrap<?> wildcardBootstrap, Namespace namespace) throws Exception
    {
        final Bootstrap<FalloutConfiguration> bootstrap = (Bootstrap<FalloutConfiguration>) wildcardBootstrap;

        final FalloutConfigurationFactoryFactory falloutConfigurationFactoryFactory =
            new FalloutConfigurationFactoryFactory(bootstrap.getConfigurationFactoryFactory(),
                falloutConfiguration -> updateConfiguration(falloutConfiguration, namespace));

        bootstrap.setConfigurationFactoryFactory(falloutConfigurationFactoryFactory);

        super.run(wildcardBootstrap, namespace);
    }

    /** Called to update configuration immediately after parsing */
    protected void updateConfiguration(FalloutConfiguration falloutConfiguration, Namespace namespace)
    {
        falloutConfiguration.updateLogFormat();
        falloutConfiguration.updateLogDir();
        falloutConfiguration.updateMetricsSuffix();
    }

    @Override
    protected void run(Environment environment, Namespace namespace,
        FalloutConfiguration configuration) throws Exception
    {
        super.run(environment, namespace, configuration);

        if (namespace.getBoolean(PID_FILE_OPTION_NAME))
        {
            writeLongToEphemeralFile(configuration.getPidFile(), getPid());
        }
    }

    private static long getPid()
    {
        return ProcessHandle.current().pid();
    }

    protected static void writeLongToEphemeralFile(Path file, long value)
    {
        try
        {
            Files.write(file, Long.toString(value).getBytes(StandardCharsets.UTF_8));
            file.toFile().deleteOnExit();
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Couldn't write to %s", file), e);
        }
    }
}
