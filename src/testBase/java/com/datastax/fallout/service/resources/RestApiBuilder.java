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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;

import java.net.URI;
import java.util.function.Supplier;

import static com.datastax.fallout.service.views.FalloutView.uriFor;

public class RestApiBuilder
{
    private final Supplier<Client> clientBuilder;
    private final URI targetUri;
    private volatile Client client;

    public RestApiBuilder(Supplier<Client> clientBuilder, URI targetUri)
    {
        this.clientBuilder = clientBuilder;
        this.targetUri = targetUri;
    }

    public RestApiBuilder connectTo(URI targetUri)
    {
        return new RestApiBuilder(clientBuilder, targetUri);
    }

    private Client getClient()
    {
        if (client == null)
        {
            client = clientBuilder.get();
        }
        return client;
    }

    public WebTarget target(String path)
    {
        return getClient()
            .target(targetUri)
            .path(path.startsWith("/") ? path.substring(1) : path);
    }

    public WebTarget target(Class clazz, String method, Object... params)
    {
        return target(uriFor(clazz, method, params).toString());
    }

    public Invocation.Builder build(String path)
    {
        return target(path).request();
    }

    public Invocation.Builder build(Class clazz, String method, Object... params)
    {
        return target(clazz, method, params).request();
    }
}
