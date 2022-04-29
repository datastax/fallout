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
package com.datastax.fallout.components.common.provider;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;

public abstract class ServiceContactPointProvider extends Provider
{
    private final String serviceName;

    public ServiceContactPointProvider(Node node, String serviceName)
    {
        super(node, false);
        this.serviceName = serviceName;
        // Register after serviceName is set so name() used to register on node does not contain null
        register();
    }

    /**
     Returns an IP address, host name, or service which can be used to connect to the service
     */
    public abstract String getContactPoint();

    public String getServiceName()
    {
        return this.serviceName;
    }

    @Override
    public String name()
    {
        return String.format("%s_contact_point_provider", this.serviceName);
    }

    public static class StaticServiceContactPointProvider extends ServiceContactPointProvider
    {
        private final String contactPoint;

        public StaticServiceContactPointProvider(Node node, String serviceName, String contactPoint)
        {
            super(node, serviceName);
            this.contactPoint = contactPoint;
        }

        @Override
        public String getContactPoint()
        {
            return this.contactPoint;
        }
    }
}
