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
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.db.CassandraDriverManager;

public class Cassandra<FC extends FalloutConfiguration> extends ConfiguredCommand<FC>
{
    public Cassandra()
    {
        super("cassandra", "Start Cassandra only.");
    }

    @Override
    protected void run(Bootstrap<FC> bootstrap, Namespace namespace,
        FC configuration) throws Exception
    {
        CassandraDriverManager driverManager = new CassandraDriverManager(configuration.getCassandraHost(),
            configuration.getCassandraPort(), configuration.getKeyspace(),
            CassandraDriverManager.SchemaMode.USE_EXISTING_SCHEMA, ignored -> {});

        driverManager.start();
    }
}
