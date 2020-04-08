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
package com.datastax.fallout.service.resources;

import java.net.URL;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;

import com.datastax.driver.core.Session;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.util.Exceptions;

/** Like {@link FalloutClient} except that it embeds its own C* {@link Session} */
public class FalloutClientWithEmbeddedSession implements AutoCloseable
{
    private final Supplier<FalloutClient> falloutClientCreator;
    private CassandraDriverManager cassandraDriverManager;
    private final Supplier<CassandraDriverManager> cassandraDriverManagerCreator;

    public FalloutClientWithEmbeddedSession(String host, int falloutPort, int cassandraPort, String keyspace)
    {
        cassandraDriverManagerCreator = Suppliers.memoize(() -> {
            cassandraDriverManager = new CassandraDriverManager(host, cassandraPort, keyspace,
                CassandraDriverManager.SchemaMode.USE_EXISTING_SCHEMA, ignored -> {});
            Exceptions.runUnchecked(cassandraDriverManager::start);
            return cassandraDriverManager;
        });

        falloutClientCreator = Suppliers.memoize(() -> new FalloutClient(
            Exceptions.getUnchecked(() -> new URL("http", host, falloutPort, "/").toURI()),
            () -> cassandraDriverManagerCreator.get().getSession()));
    }

    public RestApiBuilder anonApi()
    {
        return falloutClientCreator.get().anonApi();
    }

    public RestApiBuilder userApi()
    {
        return falloutClientCreator.get().userApi();
    }

    public RestApiBuilder adminApi()
    {
        return falloutClientCreator.get().adminApi();
    }

    @Override
    public void close() throws Exception
    {
        if (cassandraDriverManager != null)
        {
            cassandraDriverManager.stop();
        }
    }
}
