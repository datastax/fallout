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
package com.datastax.fallout.service;

import javax.validation.Validator;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.configuration.ConfigurationException;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationFactoryFactory;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.logging.AbstractAppenderFactory;

import com.datastax.fallout.ops.JobFileLoggers;

/** Overrides {@link ConfigurationFactory#build} to set {@link JobFileLoggers#FALLOUT_PATTERN} as
 *  the default logging pattern.
 *
 *  <p>We can't just set the root logger format, since dropwizard will set its own format for every
 *  appender and then override if {@link AbstractAppenderFactory#getLogFormat} is set (see implementations of {@link
 *  io.dropwizard.logging.DropwizardLayout#DropwizardLayout} and {@link AbstractAppenderFactory#buildLayout})
 *
 *  <p>FactoryFactory feels like too much indirection, so here's an explanation of what each level is
 *  doing:
 *
 *  <ul>
 *
 *  <li>A {@link ConfigurationFactory} is responsible for creating a {@link FalloutConfiguration}
 *  from a {@link ConfigurationSourceProvider}, {@link File} or providing a default configuration using its
 *  {@link ConfigurationFactory#build} methods.  How it does this is left up to the classes implementing
 *  {@link ConfigurationFactory}
 *
 *  <li>dropwizard provides for creating our own implementation by creating said
 *  classes via a Factory method, {@link ConfigurationFactoryFactory}, that is set via
 *  {@link io.dropwizard.setup.Bootstrap#setConfigurationFactoryFactory(ConfigurationFactoryFactory)}.
 *
 *  <li>We need to provide our own {@link ConfigurationFactory} implementation, {@link
 *  FalloutConfigurationFactory} so that we can intercept {@link FalloutConfiguration} creation to set
 *  default logging formats and do other post-creation and pre-use modifications via a post-build hook.
 *
 *  <li>This means we need to have our own {@link ConfigurationFactoryFactory}
 *  implementation {@link FalloutConfigurationFactoryFactory}, so we can vend a
 *  {@link FalloutConfigurationFactory} at the right point in the dropwizard startup.
 *
 *  </ul>
 */
public class FalloutConfigurationFactoryFactory<FC extends FalloutConfiguration>
    implements ConfigurationFactoryFactory<FC>
{
    private final ConfigurationFactoryFactory<FC> delegate;
    private final Consumer<FC> postBuildHook;

    public FalloutConfigurationFactoryFactory(ConfigurationFactoryFactory<FC> delegate,
        Consumer<FC> postBuildHook)
    {
        this.delegate = delegate;
        this.postBuildHook = postBuildHook;
    }

    @Override
    public FalloutConfigurationFactory create(Class<FC> klass, Validator validator,
        ObjectMapper objectMapper, String propertyPrefix)
    {
        return new FalloutConfigurationFactory(
            delegate.create(klass, validator, objectMapper, propertyPrefix));
    }

    private class FalloutConfigurationFactory implements ConfigurationFactory<FC>
    {
        private final ConfigurationFactory<FC> delegate;

        FalloutConfigurationFactory(ConfigurationFactory<FC> delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public FC build(ConfigurationSourceProvider provider,
            String path) throws IOException, ConfigurationException
        {
            return doPostBuildActions(delegate.build(provider, path));
        }

        @Override
        public FC build(File file) throws IOException, ConfigurationException
        {
            return doPostBuildActions(delegate.build(file));
        }

        @Override
        public FC build() throws IOException, ConfigurationException
        {
            return doPostBuildActions(delegate.build());
        }

        private FC doPostBuildActions(FC configuration)
        {
            postBuildHook.accept(configuration);
            return configuration;
        }
    }
}
