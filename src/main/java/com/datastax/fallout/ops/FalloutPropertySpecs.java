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
package com.datastax.fallout.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * System-wide property specs that Provisioners, ConfigurationManagers, etc. can use/expect
 */
public class FalloutPropertySpecs
{
    private static final Logger logger = LoggerFactory.getLogger(FalloutPropertySpecs.class);
    public static final String prefix = "fallout.system.";

    public static final PropertySpec<String> publicKeyPropertySpec = PropertySpecBuilder.createStr(prefix)
        .name("user.publickey")
        .description("The public key specified under caller's user profile")
        .required()
        .internal()
        .build();

    public static final PropertySpec<String> generatedClusterNamePropertySpec = PropertySpecBuilder.createName(prefix)
        .name("ui.cluster.name")
        .description("Cluster name")
        .required()
        .internal()
        .build();

    public static final PropertySpec<NodeGroup.State> launchRunLevelPropertySpec =
        PropertySpecBuilder.createEnum(prefix, NodeGroup.State.class)
            .name("runlevel")
            .description("Launch runlevel for cluster on test start")
            .defaultOf(NodeGroup.State.STARTED_SERVICES_RUNNING)
            .options(NodeGroup.State.DESTROYED,
                NodeGroup.State.CREATED,
                NodeGroup.State.STARTED_SERVICES_UNCONFIGURED,
                NodeGroup.State.STARTED_SERVICES_CONFIGURED,
                NodeGroup.State.STARTED_SERVICES_RUNNING)
            .internal()
            .build();

    public static final PropertySpec<String> testRunUrl = PropertySpecBuilder.createStr(prefix)
        .name("testrun.url")
        .description("URL of the testrun")
        .internal()
        .build();

    public static final PropertySpec<String> testRunId = PropertySpecBuilder.createStr(prefix)
        .name("testrun.id")
        .description("ID of the testrun")
        .internal()
        .build();

    public static final PropertySpec<String> testOwner = PropertySpecBuilder.createStr(prefix)
        .name("test.owner")
        .description("Owner of the test")
        .internal()
        .build();

    public static final PropertySpec<String> testName = PropertySpecBuilder.createStr(prefix)
        .name("test.name")
        .description("Name of the test")
        .internal()
        .build();
}
