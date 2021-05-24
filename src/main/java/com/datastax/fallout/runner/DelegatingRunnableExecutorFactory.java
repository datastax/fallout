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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;

import java.util.function.Function;

import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.resources.runner.RunnerResource;

import static com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import static com.datastax.fallout.service.views.FalloutView.uriFor;

/** Delegates running TestRuns to the specified delegateRunner.  The {@link DelegatingRunnableExecutor}
 *  retrieves the current TestRun value directly from the database.  The {@link DelegatingRunnableExecutor#getTestRunStatus()}
 *  implementation receives updates using {@link RunnerResource#statusFeed}; it doesn't update the TestRun though, as
 *  its assumed that the update has already been performed by the delegate runner. */
public class DelegatingRunnableExecutorFactory extends AbstractDelegatingExecutorFactory
    implements RunnableExecutorFactory
{
    private static final Logger logger = LoggerFactory.getLogger(DelegatingRunnableExecutorFactory.class);

    public DelegatingRunnableExecutorFactory(WebTarget runner, Function<TestRunIdentifier, TestRun> getTestRun)
    {
        super(runner, getTestRun);
    }

    @Override
    public DelegatingRunnableExecutor create(TestRun testRun, UserCredentials userCredentials)
    {
        return new DelegatingRunnableExecutor(testRun, userCredentials);
    }

    /** Redirect {@link RunnableExecutorFactory}'s {@link AutoCloseable#close} implementation to
     *  {@link AbstractDelegatingExecutorFactory}'s {@link Managed#stop} implementation */
    @Override
    public void close()
    {
        stop();
    }

    private class DelegatingRunnableExecutor
        extends DelegatingExecutor implements RunnableExecutorFactory.RunnableExecutor
    {
        private final RunnerResource.RunParams runParams;

        private DelegatingRunnableExecutor(TestRun testRun, UserCredentials userCredentials)
        {
            super(
                getRunner(),
                DelegatingRunnableExecutorFactory.this::getTestRun,
                getTestRunStatusUpdatePublisher(),
                testRun.getTestRunIdentifier(),
                testRun.getState());
            runParams = new RunnerResource.RunParams(testRun, userCredentials);
        }

        @Override
        public void run()
        {
            final WebTarget run = getRunner()
                .path(uriFor(RunnerResource.class, "run").toString());

            try
            {
                logger.info("{}: running testrun {} via {}", getRunner().getUri(),
                    runParams.testRun.getTestRunIdentifier(), run.getUri());
                run.request().post(Entity.json(runParams), Void.class);
            }
            catch (Throwable ex)
            {
                String errorMessage = "";

                // Jersey re-throws any WebApplicationException wrapped by ProcessingException
                if (ex instanceof WebApplicationException)
                {
                    errorMessage = ": " + ((WebApplicationException) ex).getResponse().readEntity(String.class);
                }

                throw new RuntimeException(String.format("Could not run testrun %s via %s%s",
                    runParams.testRun.getTestRunIdentifier(), run.getUri(), errorMessage), ex);
            }
        }
    }
}
