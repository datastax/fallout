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

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;

public class FalloutQueueCommand extends FalloutServerCommand
{
    public static final String DELEGATE_RUNNER_ID_OPTION_NAME = "runnerId";

    public FalloutQueueCommand(FalloutService falloutService)
    {
        super(falloutService, "queue",
            "Run fallout as a queue process, delegating testruns to a runner process");
    }

    @Override
    public void configure(Subparser subparser)
    {
        subparser.addArgument("runner-id")
            .help("Runner ID to send new testruns to")
            .dest(DELEGATE_RUNNER_ID_OPTION_NAME)
            .type(Integer.class);

        super.configure(subparser);
    }

    @Override
    protected void updateConfiguration(FalloutConfiguration falloutConfiguration, Namespace namespace)
    {
        falloutConfiguration.setQueueMode(namespace.getInt(DELEGATE_RUNNER_ID_OPTION_NAME));

        super.updateConfiguration(falloutConfiguration, namespace);
    }
}
