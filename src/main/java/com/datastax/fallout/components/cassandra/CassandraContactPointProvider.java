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
package com.datastax.fallout.components.cassandra;

import com.datastax.fallout.components.common.provider.ServiceContactPointProvider;
import com.datastax.fallout.ops.Node;

public class CassandraContactPointProvider extends ServiceContactPointProvider
{

    public CassandraContactPointProvider(Node node, String contactPoint)
    {
        super(node, contactPoint, "cassandra");
    }

    protected CassandraContactPointProvider(Node node, String contactPoint, String serviceName)
    {
        super(node, contactPoint, serviceName);
    }

    protected CassandraContactPointProvider(Node node, String contactPoint, String serviceName, boolean autoRegister)
    {
        super(node, contactPoint, serviceName, autoRegister);
    }

    @Override
    public String name()
    {
        return "cassandra_contact_point_provider";
    }
}
