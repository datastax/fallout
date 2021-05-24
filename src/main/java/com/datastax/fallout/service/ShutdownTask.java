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

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import io.dropwizard.servlets.tasks.Task;

import com.datastax.fallout.runner.AsyncShutdownHandler;

class ShutdownTask extends Task
{
    private final AsyncShutdownHandler shutdownHandler;

    public ShutdownTask(Runnable shutdownHandler)
    {
        super("shutdown");
        this.shutdownHandler = new AsyncShutdownHandler(shutdownHandler);
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) throws Exception
    {
        shutdownHandler.startShutdown();
    }
}
