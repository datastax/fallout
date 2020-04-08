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
package com.datastax.fallout.service;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.server.DefaultServerFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerWrapper;

@JsonTypeName("default")
public class DefaultServerWithHandlerFactory extends DefaultServerFactory implements ServerWithHandlerFactory
{
    private HandlerWrapper handlerWrapper;

    public DefaultServerWithHandlerFactory()
    {
        setDetailedJsonProcessingExceptionMapper(true);
    }

    /** {@link DefaultServerFactory#addRequestLog} is a suitable hook for inserting our additional handlers as it's
     * called from dropwizard's {@link DefaultServerFactory#build} just before calling {@link Server#setHandler} */
    @Override
    protected Handler addRequestLog(Server server, Handler handler, String name)
    {
        Handler requestLog = super.addRequestLog(server, handler, name);
        if (handlerWrapper != null)
        {
            ((HandlerWrapper) requestLog).insertHandler(handlerWrapper);
        }
        return requestLog;
    }

    @Override
    public void insertHandler(HandlerWrapper handlerWrapper)
    {
        if (this.handlerWrapper == null)
        {
            this.handlerWrapper = handlerWrapper;
        }
        else
        {
            this.handlerWrapper.insertHandler(handlerWrapper);
        }
    }
}
