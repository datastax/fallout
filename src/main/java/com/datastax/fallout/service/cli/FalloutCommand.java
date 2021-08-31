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

import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutConfigurationFactoryFactory;
import com.datastax.fallout.service.FalloutServiceBase;

/** A command that does not need a {@link com.datastax.fallout.service.FalloutService} instance */
public abstract class FalloutCommand<FC extends FalloutConfiguration> extends FalloutConfiguredCommand<FC>
{
    protected FalloutCommand(FalloutServiceBase<FC> application, String name, String description)
    {
        super(application, name, description);
    }

    private void forceConsoleLoggingOnly(Bootstrap<FC> bootstrap)
    {
        final FalloutConfigurationFactoryFactory<FC> falloutConfigurationFactoryFactory =
            new FalloutConfigurationFactoryFactory<>(bootstrap.getConfigurationFactoryFactory(),
                falloutConfiguration -> {
                    falloutConfiguration.forceLoggingToConsoleOnly();
                    falloutConfiguration.updateAppLogFormat();
                });

        bootstrap.setConfigurationFactoryFactory(falloutConfigurationFactoryFactory);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run(Bootstrap<?> wildcardBootstrap, Namespace namespace) throws Exception
    {
        forceConsoleLoggingOnly((Bootstrap<FC>) wildcardBootstrap);

        super.run(wildcardBootstrap, namespace);
    }
}
