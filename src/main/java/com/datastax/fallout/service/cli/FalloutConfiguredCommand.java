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

import io.dropwizard.cli.ConfiguredCommand;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;

/** Delegates {@link #getConfigurationClass} to a non-generic subclass instance of {@link FalloutServiceBase},
 *  since several subclasses of {@link FalloutConfiguredCommand} do not have non-generic subclasses and thus
 *  the default reflection-based implementation of {@link ConfiguredCommand#getConfigurationClass()} will
 *  not work.   Basically, this is a copy of the technique used in {@link io.dropwizard.cli.ServerCommand} */
public abstract class FalloutConfiguredCommand<FC extends FalloutConfiguration> extends ConfiguredCommand<FC>
{
    private final FalloutServiceBase<FC> application;

    protected FalloutConfiguredCommand(FalloutServiceBase<FC> application, String name, String description)
    {
        super(name, description);
        this.application = application;
    }

    @Override
    protected Class<FC> getConfigurationClass()
    {
        return application.getConfigurationClass();
    }
}
