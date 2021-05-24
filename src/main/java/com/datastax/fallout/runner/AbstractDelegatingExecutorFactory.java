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
package com.datastax.fallout.runner;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.sse.SseEventSource;

import java.util.function.Function;

import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.resources.ServerSentEvents.EventConsumer;
import com.datastax.fallout.service.resources.runner.RunnerResource;

import static com.datastax.fallout.service.resources.ServerSentEvents.readEvent;
import static com.datastax.fallout.service.views.FalloutView.uriFor;

public abstract class AbstractDelegatingExecutorFactory implements Managed
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractDelegatingExecutorFactory.class);

    private final WebTarget runner;
    private final Function<TestRunIdentifier, TestRun> getTestRun;
    private final SseEventSource eventSource;
    private final TestRunStatusUpdatePublisher testRunStatusUpdatePublisher = new TestRunStatusUpdatePublisher();

    public AbstractDelegatingExecutorFactory(
        WebTarget runner, Function<TestRunIdentifier, TestRun> getTestRun)
    {
        this.runner = runner;
        this.getTestRun = getTestRun;
        eventSource = SseEventSource
            .target(runner.path(uriFor(RunnerResource.class, "statusFeed").toString()))
            .build();
        eventSource.register(
            event -> readEvent(event,
                EventConsumer.of(TestRunStatusUpdate.class,
                    this.testRunStatusUpdatePublisher::publish),
                EventConsumer.of(RunnerResource.AllTestRunStatusesSent.class,
                    ignored -> markAllExistingTestRunsKnown())),
            e -> logger.error("Unexpected error in TestRunStatusUpdate feed", e),
            () -> {
                logger.info("{}: Detected shutdown", runner.getUri());
                stop();
            });
    }

    @Override
    public void start()
    {
        if (!eventSource.isOpen())
        {
            logger.info("{}: {} connecting", getRunner().getUri(), getClass().getSimpleName());
            eventSource.open();
        }
    }

    @Override
    public void stop()
    {
        logger.info("{}: {} disconnecting", getRunner().getUri(), getClass().getSimpleName());
        eventSource.close();
    }

    /** Called to indicate that we've received a {@link RunnerResource.AllTestRunStatusesSent} message */
    protected void markAllExistingTestRunsKnown()
    {
    }

    protected WebTarget getRunner()
    {
        return runner;
    }

    protected TestRun getTestRun(TestRunIdentifier testRunIdentifier)
    {
        return getTestRun.apply(testRunIdentifier);
    }

    protected TestRunStatusUpdatePublisher getTestRunStatusUpdatePublisher()
    {
        return testRunStatusUpdatePublisher;
    }
}
