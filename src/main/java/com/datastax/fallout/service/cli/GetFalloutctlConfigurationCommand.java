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

import java.util.Optional;

import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

import com.datastax.fallout.service.FalloutConfiguration;

import static java.lang.System.out;

public class GetFalloutctlConfigurationCommand extends FalloutCommand
{
    public GetFalloutctlConfigurationCommand()
    {
        super("get-falloutctl-configuration",
            "Prints various internal constants for use in falloutctl");
    }

    @Override
    protected void run(Bootstrap<FalloutConfiguration> bootstrap, Namespace namespace,
        FalloutConfiguration configuration) throws Exception
    {
        out.println("PID_FILE=" + FalloutConfiguration.PID_FILE);
        out.println("PORT_FILE=" + FalloutConfiguration.PORT_FILE);

        out.println("SERVER_LOG_DIR=" + configuration.getLogDir(Optional.empty()));
        out.println("SERVER_RUN_DIR=" + configuration.getRunDir(Optional.empty()));
        out.println("SERVER_PID_FILE=" + configuration.getPidFile(Optional.empty()));

        out.println("RUNNER_BASE_LOG_DIR=" + configuration.getLogDir(Optional.of(0)).getParent());
        out.println("RUNNER_BASE_RUN_DIR=" + configuration.getRunDir(Optional.of(0)).getParent());
    }
}
