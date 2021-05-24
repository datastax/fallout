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
package com.datastax.fallout.components.common.configuration_manager;

import com.google.auto.service.AutoService;

import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.NodeGroup;

/**
 * Does nothing. Used when no config managment is needed.
 */
@AutoService(ConfigurationManager.class)
public class NoopConfigurationManager extends ConfigurationManager
{
    static final String prefix = "fallout.configuration.management.noop.";

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "noop";
    }

    @Override
    public String description()
    {
        return "Use when no configuration management is needed";
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return NodeGroup.State.STARTED_SERVICES_RUNNING;
    }
}
