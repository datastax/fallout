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
package com.datastax.fallout.harness;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.FilenameUtils;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.ResourceRequirement;
import com.datastax.fallout.runner.ActiveTestRunFactory;
import com.datastax.fallout.runner.JobLoggersFactory;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.runner.ResourceReservationLocks;
import com.datastax.fallout.runner.ThreadedRunnableExecutorFactory;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.runner.queue.InMemoryPendingQueue;
import com.datastax.fallout.runner.queue.TestRunQueue;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.component_discovery.MockingComponentFactory;

public class TestRunnerTestHelpers
{
    public static Test makeTest(String owner, String resourcePath)
    {
        return Test.createTest(
            owner,
            FilenameUtils.getBaseName(resourcePath),
            EnsembleFalloutTest.readSharedYamlFile("/testrunner-test-yamls/" + resourcePath));
    }

    protected static class TestRunQueueThatDiscardsRequeuedTestRuns extends TestRunQueue
    {
        private final BlockingQueue<TestRun> requeuedJobs = new BlockingArrayQueue<>(1);

        public TestRunQueueThatDiscardsRequeuedTestRuns()
        {
            super(new InMemoryPendingQueue(), (testRun, ex) -> {}, List::of, Duration.ofMinutes(1), testRun -> true);
        }

        @Override
        public void take(BiConsumer<TestRun, UnprocessedHandler> consumer)
        {
            super.take((testRun, unprocessedHandler) -> consumer.accept(testRun, new UnprocessedHandler() {
                @Override
                public void requeue()
                {
                    requeuedJobs.add(testRun);
                }

                @Override
                public void handleException(Throwable ex)
                {
                    unprocessedHandler.handleException(ex);
                }
            }));
        }

        public TestRun takeRequeuedJob() throws InterruptedException
        {
            return logger.withScopedInfo("takeRequeuedJob").get(() -> requeuedJobs.poll(1, TimeUnit.MINUTES));
        }

        public boolean hasNoRequeuedJobs()
        {
            return requeuedJobs.isEmpty();
        }
    }

    @ExtendWith(MockitoExtension.class)
    public static class QueuingTestRunnerTest<FC extends FalloutConfiguration> extends TestHelpers.FalloutTest<FC>
    {
        protected User user;
        protected Fakes.TestRunFactory testRunFactory = new Fakes.TestRunFactory();

        protected TestRunQueueThatDiscardsRequeuedTestRuns testRunnerJobQueue;

        /**
         *  Static Clojure initialisation within ActiveTestRun takes a few seconds; do it up front here
         */
        @BeforeAll
        public static void initClojure()
        {
            JepsenApi.preload();
        }

        @BeforeEach
        public void setUp()
        {
            user = getTestUser();
            testRunnerJobQueue = new TestRunQueueThatDiscardsRequeuedTestRuns();
        }

        public Test makeTest(String resourcePath)
        {
            return TestRunnerTestHelpers.makeTest(getTestUser().getEmail(), resourcePath);
        }

        public class TestRunnerBuilder
        {
            private TestRunQueue testRunQueue = testRunnerJobQueue;
            private Consumer<TestRun> testRunUpdater = testRun -> {};
            private Consumer<MockingComponentFactory> componentFactoryModifier = mockingComponentFactory -> {};
            private Consumer<ActiveTestRunFactory> activeTestRunFactoryModifier = activeTestRunFactory -> {};
            private Consumer<TestRun> testRunCompletionCallback = testRun -> {};
            private ResourceReservationLocks resourceReservationLocks = new ResourceReservationLocks();
            private Function<TestRun, Set<ResourceRequirement>> getResourceRequirements =
                testRun -> Set.of();
            private Runnable processingHook = () -> {};

            public TestRunnerBuilder withResourceReservationLocks(ResourceReservationLocks resourceReservationLocks)
            {
                this.resourceReservationLocks = resourceReservationLocks;
                return this;
            }

            public TestRunnerBuilder withTestRunQueue(TestRunQueue testRunQueue)
            {
                this.testRunQueue = testRunQueue;
                return this;
            }

            public TestRunnerBuilder withTestRunUpdater(Consumer<TestRun> testRunUpdater)
            {
                this.testRunUpdater = testRunUpdater;
                return this;
            }

            public TestRunnerBuilder modifyComponentFactory(Consumer<MockingComponentFactory> componentFactoryModifier)
            {
                this.componentFactoryModifier = componentFactoryModifier;
                return this;
            }

            public TestRunnerBuilder
                modifyActiveTestRunFactory(Consumer<ActiveTestRunFactory> activeTestRunFactoryModifier)
            {
                this.activeTestRunFactoryModifier = activeTestRunFactoryModifier;
                return this;
            }

            public TestRunnerBuilder withTestRunCompletionCallback(Consumer<TestRun> testRunCompletionCallback)
            {
                this.testRunCompletionCallback = testRunCompletionCallback;
                return this;
            }

            public TestRunnerBuilder withGetResourceRequirements(
                Function<TestRun, Set<ResourceRequirement>> getResourceRequirements)
            {
                this.getResourceRequirements = getResourceRequirements;
                return this;
            }

            public TestRunnerBuilder withProcessingHook(Runnable processingHook)
            {
                this.processingHook = processingHook;
                return this;
            }

            public QueuingTestRunner build()
            {
                MockingComponentFactory componentFactory = new MockingComponentFactory();
                componentFactoryModifier.accept(componentFactory);

                JobLoggersFactory loggersFactory =
                    new JobLoggersFactory(Paths.get(falloutConfiguration().getArtifactPath()), true);

                ActiveTestRunFactory activeTestRunFactory = new ActiveTestRunFactory(
                    falloutConfiguration())
                        .withComponentFactory(componentFactory);

                activeTestRunFactoryModifier.accept(activeTestRunFactory);

                final ThreadedRunnableExecutorFactory executorFactory = new ThreadedRunnableExecutorFactory(
                    loggersFactory, testRunUpdater, activeTestRunFactory,
                    falloutConfiguration()) {
                    @Override
                    public RunnableExecutor create(TestRun testRun, UserCredentials userCredentials)
                    {
                        RunnableExecutor executor = super.create(testRun, userCredentials);
                        executor.getTestRunStatus().addInactiveCallback(
                            () -> testRunCompletionCallback.accept(testRun));

                        return new RunnableExecutor() {
                            @Override
                            public TestRunStatus getTestRunStatus()
                            {
                                return executor.getTestRunStatus();
                            }

                            @Override
                            public TestRun getTestRunCopyForReRun()
                            {
                                return executor.getTestRunCopyForReRun();
                            }

                            @Override
                            public ReadOnlyTestRun getReadOnlyTestRun()
                            {
                                return executor.getReadOnlyTestRun();
                            }

                            @Override
                            public void run()
                            {
                                processingHook.run();
                                executor.run();
                            }
                        };
                    }
                };

                return new QueuingTestRunner(testRunUpdater, testRunQueue,
                    (testRun) -> new UserCredentials(getTestUser(), Optional.empty()),
                    executorFactory,
                    getResourceRequirements,
                    resourceReservationLocks);
            }
        }

        public TestRunnerBuilder testRunnerBuilder()
        {
            return new TestRunnerBuilder();
        }
    }
}
