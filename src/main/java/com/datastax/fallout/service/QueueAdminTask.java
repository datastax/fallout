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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.service.core.PeriodicTask;

public class QueueAdminTask extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(QueueAdminTask.class);

    private final QueuingTestRunner testRunner;
    private final Collection<PeriodicTask> tasks;

    public QueueAdminTask(QueuingTestRunner testRunner, Collection<PeriodicTask> tasks)
    {
        super("queue");
        this.testRunner = testRunner;
        this.tasks = tasks;
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) throws Exception
    {
        logger.info("Queue admin task invoked with parameters={}", parameters);

        if (!parameters.get("requestShutdown").isEmpty())
        {
            requestShutdown("queue admin task");
        }
        else if (!parameters.get("cancelShutdown").isEmpty())
        {
            cancelShutdown();
        }
        else
        {
            logger.error("Invalid queue admin task parameters");
        }
    }

    public void requestShutdown(String requester)
    {
        testRunner.requestShutdown(requester);
        tasks.forEach(PeriodicTask::pause);
    }

    public void cancelShutdown()
    {
        testRunner.cancelShutdown();
        tasks.forEach(PeriodicTask::run);
    }
}
