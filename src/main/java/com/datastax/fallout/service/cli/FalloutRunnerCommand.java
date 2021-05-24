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

import java.nio.file.Paths;
import java.util.Optional;

import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.eclipse.jetty.server.Server;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;

import static java.util.Optional.ofNullable;

public class FalloutRunnerCommand<FC extends FalloutConfiguration> extends FalloutServerCommand<FC>
{
    public static final String RUNNER_ID_OPTION_NAME = "runnerId";
    public static final String PORT_FILE_OPTION_NAME = "portFile";

    public FalloutRunnerCommand(FalloutServiceBase<FC> falloutService)
    {
        super(falloutService, "runner",
            "Run fallout as a runner process, accepting testruns delegated from a queue process");
    }

    @Override
    public void configure(Subparser subparser)
    {
        subparser.addArgument("runner-id")
            .help("Integer runner ID")
            .dest(RUNNER_ID_OPTION_NAME)
            .type(Integer.class);

        subparser.addArgument("--port-file")
            .help(String.format("Write the port number to %s if --runner-id is specified",
                FalloutConfiguration.getPortFile(Paths.get("$FALLOUT_HOME"), 99)
                    .toString().replace("/99/", "/RUNNER-ID/")))
            .action(Arguments.storeTrue())
            .dest(PORT_FILE_OPTION_NAME);

        super.configure(subparser);
    }

    @Override
    protected String getPidFileLocation()
    {
        return FalloutConfiguration.getPidFile(Paths.get("$FALLOUT_HOME"), Optional.of(99))
            .toString().replace("/99/", "/RUNNER-ID/");
    }

    @Override
    protected void updateConfiguration(FalloutConfiguration falloutConfiguration, Namespace namespace)
    {
        ofNullable(namespace.getInt(FalloutRunnerCommand.RUNNER_ID_OPTION_NAME))
            .ifPresent(falloutConfiguration::setRunnerMode);

        super.updateConfiguration(falloutConfiguration, namespace);
    }

    @Override
    protected void run(Environment environment, Namespace namespace,
        FC configuration) throws Exception
    {
        if (namespace.getBoolean(PORT_FILE_OPTION_NAME))
        {
            environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
                @Override
                public void serverStarted(Server server)
                {
                    writeLongToEphemeralFile(configuration.getPortFile(), getLocalPort(server));
                }
            });
        }
        super.run(environment, namespace, configuration);
    }
}
