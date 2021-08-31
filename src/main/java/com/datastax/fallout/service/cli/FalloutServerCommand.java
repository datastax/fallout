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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import io.dropwizard.cli.ServerCommand;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutConfigurationFactoryFactory;
import com.datastax.fallout.service.FalloutServiceBase;

/** A command that starts a persistent server */
public abstract class FalloutServerCommand<FC extends FalloutConfiguration> extends ServerCommand<FC>
{
    public static final String PID_FILE_OPTION_NAME = "pidFile";

    protected FalloutServerCommand(FalloutServiceBase<FC> falloutService, String name, String description)
    {
        super(falloutService, name, description);
    }

    /** Return a string representing where the PID file will be found for use in the command help in {@link #configure};
     * note that it cannot access an instance of {@link FalloutConfiguration} because that won't exist yet */
    protected abstract String getPidFileLocation();

    @Override
    public void configure(Subparser subparser)
    {
        subparser.addArgument("--pid-file")
            .help(String.format("Write the process ID to %s", getPidFileLocation()))
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
        final Bootstrap<FC> bootstrap = (Bootstrap<FC>) wildcardBootstrap;

        final FalloutConfigurationFactoryFactory<FC> falloutConfigurationFactoryFactory =
            new FalloutConfigurationFactoryFactory<>(bootstrap.getConfigurationFactoryFactory(),
                falloutConfiguration -> updateConfiguration(falloutConfiguration, namespace));

        bootstrap.setConfigurationFactoryFactory(falloutConfigurationFactoryFactory);

        super.run(wildcardBootstrap, namespace);
    }

    /** Called to update configuration immediately after parsing */
    protected void updateConfiguration(FalloutConfiguration falloutConfiguration, Namespace namespace)
    {
        falloutConfiguration.updateAppLogFormat();
        falloutConfiguration.updateLogDir();
        falloutConfiguration.updateMetricsSuffix();
    }

    @Override
    protected void run(Environment environment, Namespace namespace,
        FC configuration) throws Exception
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
            Files.writeString(file, Long.toString(value));
            file.toFile().deleteOnExit();
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Couldn't write to %s", file), e);
        }
    }
}
