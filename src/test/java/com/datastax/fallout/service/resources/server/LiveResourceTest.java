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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.SseEventSource;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.dropwizard.testing.ConfigOverride;
import org.apache.commons.lang3.tuple.Pair;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.Timeout;

import com.datastax.fallout.harness.Module;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.harness.impl.FakeModule;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.Provisioner;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;
import com.datastax.fallout.runner.CheckResourcesResult;
import com.datastax.fallout.service.artifacts.ArtifactWatcherTest;
import com.datastax.fallout.service.core.Test;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.resources.FalloutServiceRule;
import com.datastax.fallout.service.resources.ServerSentEvents;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ScopedLogger;

import static com.datastax.fallout.service.artifacts.ArtifactWatcherTest.TIMESTAMP_RESOLUTION_SECONDS;
import static com.datastax.fallout.service.core.TestRunAssert.assertThat;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.ResponseAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

public class LiveResourceTest extends TestResourceTestBase
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(LiveResourceTest.class);

    private static final int HEART_BEAT_INTERVAL_SECONDS = 1;
    private static final int COALESCING_GRANULARITY_SECONDS = 1;

    @ClassRule
    public static final FalloutServiceRule FALLOUT_SERVICE_RULE = new FalloutServiceRule(
        ConfigOverride.config("serverSentEventsHeartBeatIntervalSeconds",
            Integer.toString(HEART_BEAT_INTERVAL_SECONDS)),
        ConfigOverride.config("artifactWatcherCoalescingIntervalSeconds",
            Integer.toString(COALESCING_GRANULARITY_SECONDS)));

    @Rule
    public final FalloutServiceRule.FalloutServiceResetRule FALLOUT_SERVICE_RESET_RULE =
        FALLOUT_SERVICE_RULE.resetRule();

    @Rule
    public Timeout globalTimeout = Timeout.seconds(90);

    private WebTarget getEventStreamTarget(String owner, String testName, String testRunID, String artifactPath)
    {
        return api
            .target(LiveResource.class, "artifactEventStream", owner, testName, testRunID, artifactPath);
    }

    private WebTarget getEventStreamTarget(TestRun testRun, String artifactPath)
    {
        return getEventStreamTarget(testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId().toString(),
            artifactPath);
    }

    private Response getEventStream(String owner, String testName, String testRunID, String artifactPath)
    {
        return getEventStreamTarget(owner, testName, testRunID, artifactPath)
            .request()
            .accept("text/event-stream").get();
    }

    private Response getEventStream(TestRun testRun, String artifactPath)
    {
        return getEventStream(testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId().toString(),
            artifactPath);
    }

    private class EventStream implements AutoCloseable
    {
        private final SseEventSource eventSource;
        private final BlockingQueue<InboundSseEvent> eventQueue = new LinkedBlockingQueue<>();
        private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();
        private volatile boolean isComplete = false;

        EventStream(TestRun testRun, String artifactPath)
        {
            eventSource = SseEventSource.target(getEventStreamTarget(testRun, artifactPath)).build();
            eventSource.register(
                eventQueue::offer,
                errorQueue::offer,
                () -> {
                    isComplete = true;
                });
            eventSource.open();
        }

        boolean isComplete()
        {
            return isComplete;
        }

        @Override
        public void close() throws Exception
        {
            logger.debug("EventStream.close()");
            eventSource.close();
            assertThat(errorQueue).isEmpty();
        }

        private Optional<Pair<String, String>> readEvent()
        {
            return logger.doWithScopedDebug(() -> {
                final InboundSseEvent event = Exceptions.getUninterruptibly(eventQueue::take);
                // Empty events are heartbeats: ignore them
                return event.isEmpty() ?
                    Optional.empty() :
                    Optional.of(Pair.of(event.getName(), event.readData()));
            },
                "readEvent");
        }

        private Optional<Pair<String, String>> readUpdateEvent()
        {
            return readEvent().map(event -> {
                assertThat(event).isEqualTo(Pair.of("state", "updated"));
                return event;
            });
        }

        private void readAtLeastOneUpdateEvent()
        {
            logger.doWithScopedDebug(() -> {
                await()
                    .atMost(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .until(this::readUpdateEvent, Optional::isPresent);
                while (eventQueue.peek() != null)
                {
                    readUpdateEvent();
                }
            },
                "readAtLeastOneUpdateEvent");
        }
    }

    /** The worst case scenario is going to be events being propagated on the edge of each of these intervals i.e.
     *  they could be cumulative.  We add a fudge to the end as well. */
    private final long CLIENT_TIMEOUT_SECONDS =
        HEART_BEAT_INTERVAL_SECONDS +
            COALESCING_GRANULARITY_SECONDS +
            TIMESTAMP_RESOLUTION_SECONDS + 2;

    private CountDownLatch testStarted;
    private BlockingQueue<String> moduleLogMessages;
    private Test test;
    private TestRun testRun;
    private BlockingQueue<Boolean> streamOpenStates;

    @Before
    public void setup() throws IOException
    {
        api = FALLOUT_SERVICE_RESET_RULE.userApi();
        testStarted = new CountDownLatch(1);
        moduleLogMessages = new LinkedBlockingQueue<>();

        FALLOUT_SERVICE_RULE.componentFactory()
            .clear()
            .mockAll(Provisioner.class, () -> new FakeProvisioner()
            {
                @Override
                protected CheckResourcesResult reserveImpl(NodeGroup nodeGroup)
                {
                    logger.doWithScopedInfo(() -> testStarted.countDown(), "triggering testStarted: {}", testStarted);
                    return super.reserveImpl(nodeGroup);
                }
            })
            .mockAll(Module.class, () -> new FakeModule()
            {
                @Override
                public void run(Ensemble ensemble, PropertyGroup properties)
                {
                    try (ScopedLogger.Scoped ignored = LiveResourceTest.logger.scopedInfo("run"))
                    {
                        emit(Operation.Type.invoke);
                        while (true)
                        {
                            final String message = Exceptions.getUnchecked(() ->
                            // We may have to wait longer for a message than CLIENT_TIMEOUT_SECONDS, since
                            // there may be multiple things being polled for on the client side that wait for
                            // nearly CLIENT_TIMEOUT_SECONDS.  This is a fudge.
                            moduleLogMessages.poll(CLIENT_TIMEOUT_SECONDS * 3, TimeUnit.SECONDS));

                            if (message != null)
                            {
                                ArtifactWatcherTest.waitForTimestampResolutionDuration();
                                logger().info(message);
                            }
                            else
                            {
                                LiveResourceTest.logger.error("Timeout waiting for message in fake module");
                                emit(Operation.Type.error);
                                break;
                            }

                            if (message.equals("stop"))
                            {
                                emit(Operation.Type.ok);
                                break;
                            }
                        }
                    }
                }
            });

        test = createTest("/", "fakes").readEntity(Test.class);

        streamOpenStates = new LinkedBlockingQueue<>();
        ServerSentEvents.setStreamOpenStatesListener(e -> Exceptions.runUnchecked(() -> streamOpenStates.put(e)));
    }

    @After
    public void teardown() throws InterruptedException
    {
        stopTest();
    }

    private Boolean pollStreamOpenStates() throws InterruptedException
    {
        return streamOpenStates.poll(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private ConditionFactory await()
    {
        return Awaitility.await().atMost(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void startTest() throws InterruptedException
    {
        testRun = startTest(test.getName());
        logger.doWithScopedInfo(
            () -> Exceptions
                .runUnchecked(() -> assertThat(testStarted.await(CLIENT_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue()),
            "waiting for testStarted: {}", testStarted);
    }

    private void stopTest() throws InterruptedException
    {
        if (testRun != null)
        {
            moduleLogMessages.put("stop");
            assertThat(waitForCompletedTestRun(testRun)).hasState(TestRun.State.PASSED);
        }
    }

    private void runTest() throws InterruptedException
    {
        moduleLogMessages.put("stop");
        testRun = runTest(test.getName());
    }

    @org.junit.Test
    public void a_live_testrun_publishes_updates_and_handles_client_side_close() throws Exception
    {
        startTest();

        try (final EventStream stream = new EventStream(testRun, "fallout-shared.log"))
        {
            assertThat(pollStreamOpenStates()).isTrue();
            assertThat(streamOpenStates).isEmpty();

            stream.readAtLeastOneUpdateEvent();

            moduleLogMessages.put("update");

            stream.readAtLeastOneUpdateEvent();
        }

        assertThat(pollStreamOpenStates()).isFalse();
        assertThat(pollStreamOpenStates()).isNull();
    }

    @org.junit.Test
    public void a_live_stream_for_a_non_existent_file_returns_404() throws InterruptedException
    {
        startTest();

        assertThat(getEventStream(testRun, "nope.log")).hasStatusInfo(NOT_FOUND);

        assertThat(pollStreamOpenStates()).isNull();
    }

    @org.junit.Test
    public void a_live_stream_for_a_non_existent_testrun_returns_404()
    {
        assertThat(getEventStream(
            "humphrey.bogus@example.com", "lauren.notatall", "FA110072-D811-4B2C-B08F-000000000000", "nope.log"))
                .hasStatusInfo(NOT_FOUND);
    }

    @org.junit.Test
    public void a_non_live_testrun_responds_with_state_finished() throws Exception
    {
        runTest();
        try (final EventStream stream = new EventStream(testRun, "fallout-shared.log"))
        {
            assertThat(stream.readEvent()).hasValue(Pair.of("state", "finished"));
        }
    }

    @org.junit.Test
    public void a_live_testrun_terminates_with_state_finished() throws Exception
    {
        startTest();

        try (final EventStream stream = new EventStream(testRun, "fallout-shared.log"))
        {
            assertThat(pollStreamOpenStates()).isTrue();
            assertThat(streamOpenStates).isEmpty();

            stream.readAtLeastOneUpdateEvent();

            moduleLogMessages.put("update");

            stream.readAtLeastOneUpdateEvent();

            stopTest();

            await().untilAsserted(() -> assertThat(stream.readEvent()).hasValue(Pair.of("state", "finished")));

            // SseEventSource will keep attempting to connect on server-side close unless we close the client side i.e.
            // we have to detect the change to "finished" and manually close the stream.
            stream.close();

            await().untilAsserted(() -> assertThat(stream.isComplete()).isTrue());
        }

        assertThat(pollStreamOpenStates()).isFalse();
        assertThat(pollStreamOpenStates()).isNull();
    }
}
