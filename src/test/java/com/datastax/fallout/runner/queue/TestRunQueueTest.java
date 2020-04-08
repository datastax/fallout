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
package com.datastax.fallout.runner.queue;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ScopedLogger;

import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class TestRunQueueTest
{
    private Fakes.UUIDFactory uuidFactory = new Fakes.UUIDFactory();
    private Date createdAt = new Date();
    private static final ScopedLogger logger = ScopedLogger.getLogger(TestRunQueueTest.class);

    private TestRun createTestRun()
    {
        TestRun testRun = new TestRun()
        {
            @Override
            public String toString()
            {
                return String.valueOf(getTestRunId().getLeastSignificantBits());
            }
        };
        testRun.setTestRunId(uuidFactory.create());
        testRun.setOwner(TEST_USER_EMAIL);
        createdAt = Date.from(createdAt.toInstant().plusSeconds(1));
        testRun.setCreatedAt(createdAt);
        return testRun;
    }

    private TestRunQueue queue;
    private BlockingQueue<Boolean> takerRelease;
    private BlockingQueue<TestRun> taken;
    private AtomicBoolean taking;
    private CompletableFuture<Void> taker;

    private static final long EXPECTED_SUCCESSFUL_OPERATION_TIMEOUT_SECONDS = 60;
    private final long EXPECTED_FAILED_OPERATION_TIMEOUT_SECONDS = 2;

    private static <T> T pollUninterruptibly(BlockingQueue<T> queue, long timeoutSeconds)
    {
        while (true)
        {
            try
            {
                return queue.poll(timeoutSeconds, TimeUnit.SECONDS);
            }
            catch (InterruptedException ignored)
            {
            }
        }
    }

    private static <T> T pollUninterruptibly(BlockingQueue<T> queue)
    {
        return pollUninterruptibly(queue, EXPECTED_SUCCESSFUL_OPERATION_TIMEOUT_SECONDS);
    }

    private static <T> void offerUninterruptibly(BlockingQueue<T> queue, T element)
    {
        while (true)
        {
            try
            {
                assertThat(queue.offer(element, EXPECTED_SUCCESSFUL_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
                return;
            }
            catch (InterruptedException ignored)
            {
            }
        }
    }

    @Before
    public void setup()
    {
        queue = new TestRunQueue(new InMemoryPendingQueue(), List::of, Duration.ofMillis(1), testRun -> true);
        takerRelease = new ArrayBlockingQueue<>(1);
        taken = new ArrayBlockingQueue<>(1);
        taking = new AtomicBoolean(true);
    }

    private void releaseTaker()
    {
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("releaseTaker"))
        {
            offerUninterruptibly(takerRelease, true);
        }
    }

    private void waitForRelease()
    {
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("waitForRelease"))
        {
            assertThat(pollUninterruptibly(takerRelease)).isTrue();
        }
    }

    private void startTaker()
    {
        final CountDownLatch takerStarted = new CountDownLatch(1);
        taker = CompletableFuture.runAsync(() -> {
            takerStarted.countDown();
            while (taking.get())
            {
                waitForRelease();
                queue.take((testRun, requeueTestRun) -> {
                    try (ScopedLogger.Scoped ignored =
                        logger.scopedInfo("take offering (" + testRun.getShortName() + ")"))
                    {
                        offerUninterruptibly(taken, testRun);
                    }
                });
            }
        });
        Uninterruptibles.awaitUninterruptibly(takerStarted);
    }

    private TestRun getProcessedTestRun()
    {
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("getProcessedTestRun"))
        {
            releaseTaker();
            return pollUninterruptibly(taken);
        }
    }

    private void assertNoProcessedAvailable()
    {
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("assertNoProcessAvailable"))
        {
            releaseTaker();
            assertThat(pollUninterruptibly(taken, EXPECTED_FAILED_OPERATION_TIMEOUT_SECONDS)).isNull();
        }
    }

    @After
    public void teardown()
    {
        try (ScopedLogger.Scoped ignored = logger.scopedInfo("teardown"))
        {
            taking.set(false);
            releaseTaker();
            await().until(() -> {
                queue.unblock();
                return taker.isDone();
            });
        }
    }

    @Test
    public void jobs_are_queued_but_not_processed_when_paused_at_start()
    {
        queue.pause();

        startTaker();

        List<TestRun> testRuns = IntStream.range(0, 3)
            .mapToObj(ignored -> createTestRun())
            .collect(Collectors.toList());

        testRuns.forEach(queue::add);

        assertThat(getQueuedTestRunsSize()).isEqualTo(testRuns.size());
        assertThat(taken).isEmpty();

        queue.resume();

        List<TestRun> takenTestRuns = testRuns.stream()
            .map(ignored -> getProcessedTestRun())
            .collect(Collectors.toList());

        assertThat(takenTestRuns).isEqualTo(testRuns);
    }

    private int getQueuedTestRunsSize()
    {
        return queue.getQueuedTestRuns().size();
    }

    @Test
    public void jobs_are_queued_but_not_processed_when_paused_partway()
    {
        startTaker();

        List<TestRun> testRuns = IntStream.range(0, 6)
            .mapToObj(ignored -> createTestRun())
            .collect(Collectors.toList());

        testRuns.forEach(queue::add);

        assertThat(getQueuedTestRunsSize()).isEqualTo(testRuns.size());

        List<TestRun> takenTestRuns = IntStream.range(0, 3)
            .mapToObj(ignored -> getProcessedTestRun())
            .collect(Collectors.toList());

        queue.pause();

        assertNoProcessedAvailable();

        queue.resume();

        takenTestRuns.addAll(IntStream.range(0, 3)
            .mapToObj(ignored -> getProcessedTestRun())
            .collect(Collectors.toList()));

        assertThat(getQueuedTestRunsSize()).isZero();

        assertNoProcessedAvailable();

        assertThat(takenTestRuns).isEqualTo(testRuns);
    }

    @Test
    public void the_queue_can_be_unblocked_when_paused()
    {
        startTaker();
        assertNoProcessedAvailable();
        queue.pause();
    }
}
