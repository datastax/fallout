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
package com.datastax.fallout.service.db;

import com.datastax.fallout.service.db.CassandraDriverManager.SchemaMode;

public class CassandraDriverManagerHelpers
{
    public static void dropSchema(CassandraDriverManager cassandraDriverManager)
    {
        cassandraDriverManager.getSession().execute("DROP KEYSPACE IF EXISTS " + cassandraDriverManager.getKeySpace());

    }

    public static CassandraDriverManager createDriverManager(String keyspace)
    {
        return new CassandraDriverManager("localhost", 9096, keyspace, SchemaMode.CREATE_SCHEMA,
            CassandraDriverManagerHelpers::dropSchema);
    }
}
