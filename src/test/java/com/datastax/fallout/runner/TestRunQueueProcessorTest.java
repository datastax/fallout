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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.datastax.fallout.harness.TestRunStatus;
import com.datastax.fallout.runner.queue.ReadOnlyTestRunQueue;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.NamedThreadFactory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
public class TestRunQueueProcessorTest
{
    private SingleShotTestRunQueue testRunQueue;
    private ExecutorService jobQueueProcessorExecutor;
    private TestRunQueueProcessor testRunQueueProcessor;

    @Mock
    private ReadOnlyTestRunQueue.UnprocessedHandler unprocessedHandler;

    @Mock
    private TestRunStatus testRunStatus;

    @Mock
    private RunnableExecutorFactory.RunnableExecutor executor;

    private AtomicBoolean willRequeueJob;
    private AtomicReference<Runnable> requeueJobCallback;

    private class SingleShotTestRunQueue implements ReadOnlyTestRunQueue
    {
        private CompletableFuture<TestRun> queuedJobFuture = new CompletableFuture<>();
        private CompletableFuture<TestRun> takenJobFuture = new CompletableFuture<>();

        private void addAndWaitForProcessing()
        {
            takenJobFuture = new CompletableFuture<>();
            queuedJobFuture.complete(new TestRun());
            takenJobFuture.join();
        }

        @Override
        public void take(BiConsumer<TestRun, UnprocessedHandler> consumer)
        {
            try
            {
                TestRun completedJob = queuedJobFuture.thenApplyAsync(job -> {
                    consumer.accept(job, unprocessedHandler);
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

    @BeforeEach
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

        testRunQueue = new SingleShotTestRunQueue();
        jobQueueProcessorExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("TestRunQueueProcessor"));

        final UserCredentialsFactory nullUserCredentialsFactory = testRun -> null;
        final RunnableExecutorFactory testRunExecutorFactory = (testRun, userCredentials) -> executor;

        testRunQueueProcessor = new TestRunQueueProcessor(testRunQueue, nullUserCredentialsFactory,
            testRunExecutorFactory, new ResourceReservationLocks());
        jobQueueProcessorExecutor.execute(testRunQueueProcessor);
    }

    public void executorRunWillPause()
    {
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
    }

    @AfterEach
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
        then(unprocessedHandler).should(times(handledExceptions)).handleException(any());
    }

    private void thenTheJobIsRequeued()
    {
        then(unprocessedHandler).should(times(1)).requeue();
    }

    private void thenTheJobIsNotRequeued()
    {
        then(unprocessedHandler).should(never()).requeue();
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
        executorRunWillPause();
        whenJobIsProcessed();
        thenHandledExceptionsAre(0);
        thenTheJobIsNotRequeued();
    }

    @Test
    public void requeue_callbacks_are_handled() throws InterruptedException
    {
        executorRunWillPause();
        givenJobWillCallRequeue();
        whenJobIsProcessed();
        thenHandledExceptionsAre(0);
        thenTheJobIsRequeued();
    }
}
