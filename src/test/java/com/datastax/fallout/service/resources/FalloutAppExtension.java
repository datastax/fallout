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
package com.datastax.fallout.service.resources;

import io.dropwizard.testing.ConfigOverride;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;

public class FalloutAppExtension extends FalloutAppExtensionBase<FalloutConfiguration, FalloutService>
{
    public FalloutAppExtension(ConfigOverride... configOverrides)
    {
        this(FalloutConfiguration.ServerMode.STANDALONE, null, configOverrides);
    }

    public FalloutAppExtension(FalloutConfiguration.ServerMode mode, ConfigOverride... configOverrides)
    {
        this(mode, null, configOverrides);
    }

    public FalloutAppExtension(FalloutConfiguration.ServerMode mode, String configPath,
        ConfigOverride... configOverrides)
    {
        super(FalloutService.class, mode, configPath, configOverrides);
    }
}
