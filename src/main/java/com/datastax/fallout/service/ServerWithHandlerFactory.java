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
package com.datastax.fallout.service;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.server.ServerFactory;
import org.eclipse.jetty.server.handler.HandlerWrapper;

/** This class is a hack that allows us to reliably override calls to {@link
 * ServerFactory#build} without dropwizard YAML changes breaking it.  It works like this:
 *
 * <ul>
 *  <li> To ensure that any "server:" specification without a "type:" member in the dropwizard
 *  YAML config results in a {@link DefaultServerWithHandlerFactory}, we introduce a new interface,
 *  {@link ServerWithHandlerFactory} which has a JSON defaultImpl of {@link DefaultServerWithHandlerFactory}.
 *  <li> {@link FalloutConfiguration#getServerFactory()} overrides the "server" json
 *  property in {@link io.dropwizard.Configuration} with one that is a {@link ServerWithHandlerFactory}.
 * </ul>
 *
 * The upshot is that we can safely override {@link io.dropwizard.server.DefaultServerFactory}
 * methods in {@link DefaultServerWithHandlerFactory} without having to worry about
 * yaml configurations breaking it by setting <code>server:</code> properties (which,
 * without this change, would cause {@link io.dropwizard.server.DefaultServerFactory}
 * to be created instead of <em>our</em> {@link DefaultServerWithHandlerFactory}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultServerWithHandlerFactory.class)
interface ServerWithHandlerFactory extends ServerFactory
{
    void insertHandler(HandlerWrapper handlerWrapper);
}
