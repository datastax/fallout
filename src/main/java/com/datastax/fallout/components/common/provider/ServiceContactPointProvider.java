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

public class ServiceContactPointProvider extends Provider
{
    private final String contactPoint;
    private final String serviceName;

    public ServiceContactPointProvider(Node node, String contactPoint, String serviceName)
    {
        super(node);
        this.contactPoint = contactPoint;
        this.serviceName = serviceName;
    }

    protected ServiceContactPointProvider(Node node, String contactPoint, String serviceName, boolean autoRegister)
    {
        super(node, autoRegister);
        this.contactPoint = contactPoint;
        this.serviceName = serviceName;
    }

    @Override
    public String name()
    {
        return "service_contact_point_provider";
    }

    /**
     Returns an IP address, host name, or service which can be used to connect to the service
     */
    public String getContactPoint()
    {
        return contactPoint;
    }

    public String getServiceName()
    {
        return serviceName;
    }
}
