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
package com.datastax.fallout.runner;

import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.ExceptionHandler;
import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.runner.queue.ReadOnlyTestRunQueue;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.NamedThreadFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.times;

public class TestRunQueueProcessorTest
{
    private SingleShotTestRunQueue testRunQueue;
    private ExecutorService jobQueueProcessorExecutor;
    private TestRunQueueProcessor testRunQueueProcessor;
    private Supplier<Optional<ActiveTestRun>> activator;

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private ExceptionHandler exceptionHandler;

    @Mock
    private TestRunStatus testRunStatus;

    @Mock
    private RunnableExecutorFactory.RunnableExecutor executor;

    private AtomicBoolean willRequeueJob;
    private AtomicReference<Runnable> requeueJobCallback;

    private static class SingleShotTestRunQueue implements ReadOnlyTestRunQueue
    {
        private CompletableFuture<TestRun> queuedJobFuture = new CompletableFuture<>();
        private CompletableFuture<TestRun> takenJobFuture = new CompletableFuture<>();
        private AtomicBoolean jobWasRequeued = new AtomicBoolean(false);

        private void addAndWaitForProcessing()
        {
            takenJobFuture = new CompletableFuture<>();
            jobWasRequeued.set(false);
            queuedJobFuture.complete(new TestRun());
            takenJobFuture.join();
        }

        private boolean jobWasRequeued()
        {
            return jobWasRequeued.get();
        }

        @Override
        public void take(BiConsumer<TestRun, Consumer<TestRun>> consumer)
        {
            try
            {
                TestRun completedJob = queuedJobFuture.thenApplyAsync(job -> {
                    consumer.accept(job, requeuedJob_ -> jobWasRequeued.set(true));
                    return job;
                }).join();
                takenJobFuture.complete(completedJob);
            }
            catch (CancellationException | CompletionException e)
            {
                takenJobFuture.cancel(true);
            }

            queuedJobFuture = new CompletableFuture<>();
        }

        @Override
        public void unblock()
        {
            queuedJobFuture.cancel(true);
        }
    }

    @Before
    public void setUp()
    {
        willRequeueJob = new AtomicBoolean(false);
        requeueJobCallback = new AtomicReference<>();
        given(executor.getTestRunStatus()).willReturn(testRunStatus);
        willAnswer(invocation -> {
            requeueJobCallback.set(invocation.getArgument(0));
            return null;
        })
            .given(testRunStatus).addResourcesUnavailableCallback(any());

        // Make run() pause; if shutdown() fails to wait for all tasks to finish, then this will trigger race
        // failures in the form of missing activeTestRun.close and exceptionHandler calls.
        willAnswer(ignored -> {
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
            if (willRequeueJob.get())
            {
                requeueJobCallback.get().run();
            }
            return null;
        })
            .given(executor).run();

        testRunQueue = new SingleShotTestRunQueue();
        jobQueueProcessorExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("TestRunQueueProcessor"));

        final UserCredentialsFactory nullUserCredentialsFactory = testRun -> null;
        final RunnableExecutorFactory testRunExecutorFactory = (testRun, userCredentials) -> executor;

        testRunQueueProcessor = new TestRunQueueProcessor(testRunQueue, nullUserCredentialsFactory, exceptionHandler,
            testRunExecutorFactory, new ResourceReservationLocks());
        jobQueueProcessorExecutor.execute(testRunQueueProcessor);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        testRunQueueProcessor.shutdown();
        jobQueueProcessorExecutor.shutdown();
        jobQueueProcessorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
    }

    private void givenJobWillCallRequeue()
    {
        willRequeueJob.set(true);
    }

    private void whenJobIsProcessed() throws InterruptedException
    {
        testRunQueue.addAndWaitForProcessing();
        shutdown();
    }

    private void thenHandledExceptionsAre(int handledExceptions)
    {
        then(exceptionHandler).should(times(handledExceptions)).accept(any(), any());
    }

    private void thenTheJobIsRequeued()
    {
        assertThat(testRunQueue.jobWasRequeued()).isTrue();
    }

    private void thenTheJobIsNotRequeued()
    {
        assertThat(testRunQueue.jobWasRequeued()).isFalse();
    }

    @Test
    public void exceptions_cannot_escape_from_processing() throws InterruptedException
    {
        willThrow(new RuntimeException()).given(executor).run();
        whenJobIsProcessed();
        thenHandledExceptionsAre(1);
        thenTheJobIsNotRequeued();
    }

    @Test
    public void normal_processing_is_successful() throws InterruptedException
    {
        whenJobIsProcessed();
        thenHandledExceptionsAre(0);
        thenTheJobIsNotRequeued();
    }

    @Test
    public void requeue_callbacks_are_handled() throws InterruptedException
    {
        givenJobWillCallRequeue();
        whenJobIsProcessed();
        thenHandledExceptionsAre(0);
        thenTheJobIsRequeued();
    }
}
