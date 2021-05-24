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
package com.datastax.fallout.components.impl;

import java.util.List;

import com.google.auto.service.AutoService;

import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * A No-Op configuration manager
 */
@AutoService(ConfigurationManager.class)
public class TestConfigurationManager extends ConfigurationManager
{
    static final String prefix = "test.configuration.management.";

    static final PropertySpec<String> somePropertySpec = PropertySpecBuilder.createStr(prefix, "(abc|def|hgi)")
        .name("foo")
        .description("Some random property")
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "Test Configuration Manager";
    }

    @Override
    public String description()
    {
        return name() + " - for unit testing configuration management api";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(somePropertySpec);
    }

    @Override
    protected NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        return NodeGroup.State.STARTED_SERVICES_RUNNING;
    }
}
