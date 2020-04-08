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
package com.datastax.fallout.service.artifacts;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.util.HashedWheelTimer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.NamedThreadFactory;

import static org.assertj.core.api.Assertions.assertThat;

public class ArtifactWatcherTest extends TestHelpers.FalloutTest
{
    private ArtifactWatcher watcher;
    private BlockingQueue<Path> updates;
    private HashedWheelTimer eventCoalescingTimer;

    @Before
    public void setup() throws Exception
    {
        eventCoalescingTimer = new HashedWheelTimer(new NamedThreadFactory("EventCoalescingTimer"));
        watcher = new ArtifactWatcher(testRunArtifactPath(), eventCoalescingTimer, 1);
        watcher.start();
        updates = new LinkedBlockingQueue<>();
    }

    @After
    public void teardown() throws Exception
    {
        watcher.stop();
        eventCoalescingTimer.stop();
    }

    @Rule
    public Timeout globalTimeout = Timeout.seconds(90); // On the high side because MacOS has no native WatchService implementation.

    public Path createArtifact(String relativeArtifactPath)
    {
        return testRunArtifactPath().relativize(super.createTestRunArtifact(relativeArtifactPath, "content\n"));
    }

    public static final int TIMESTAMP_RESOLUTION_SECONDS = 1;

    /** On MacOS with APFS and Oracle JDK 1.8.0_162 file timestamp resolution is 1 second, so
     *  we need to pause before updating our file to make sure that the polling WatchService
     *  implementation sees the change.  On systems with better timestamp resolution and/or a
     *  native kqueue/epoll-based WatchService, this just makes the test 1 second longer. */
    public static void waitForTimestampResolutionDuration()
    {
        Uninterruptibles.sleepUninterruptibly(TIMESTAMP_RESOLUTION_SECONDS, TimeUnit.SECONDS);
    }

    public void updateArtifacts(Path... relativeArtifactPaths)
    {
        waitForTimestampResolutionDuration();
        for (Path path : relativeArtifactPaths)
        {
            super.updateTestRunArtifact(testRunArtifactPath().resolve(path), "content\n");
        }
    }

    private void callback(Path path)
    {
        Exceptions.runUnchecked(() -> updates.put(path));
    }

    private List<Long> watch(Path... paths)
    {
        return Arrays.stream(paths).map(path -> watcher.watch(path, this::callback)).collect(Collectors.toList());
    }

    private List<Path> getExactlyNUpdates(int n)
    {
        List<Path> result = IntStream.range(0, n)
            .mapToObj(ignored -> Exceptions.getUnchecked(() -> updates.take()))
            .collect(Collectors.toList());
        assertThat(updates).isEmpty();
        return result;
    }

    private void assertUpdatesAreExactly(Path... paths)
    {
        assertThat(getExactlyNUpdates(paths.length)).containsExactlyInAnyOrder(paths);
    }

    @Test
    public void notifies_of_changes_to_multiple_artifacts_in_different_directories() throws InterruptedException
    {
        Path a = createArtifact("a");
        Path b = createArtifact("b");
        Path c = createArtifact("d/c");

        watch(a, b, c);

        updateArtifacts(a, b);
        assertUpdatesAreExactly(a, b);

        updateArtifacts(b, c);
        assertUpdatesAreExactly(b, c);
    }

    @Test
    public void handles_multiple_callbacks_for_a_single_file()
    {
        Path a = createArtifact("a");
        Path b = createArtifact("b");

        watch(a, b, a);

        updateArtifacts(a, b);
        assertUpdatesAreExactly(a, a, b);
    }

    private long useWatcherWithLongCoalescingInterval() throws Exception
    {
        final int coalescingIntervalSeconds = 5;
        watcher.stop();
        watcher = new ArtifactWatcher(testRunArtifactPath(), eventCoalescingTimer, coalescingIntervalSeconds);
        watcher.start();
        return coalescingIntervalSeconds;
    }

    @Test
    public void coalesces_multiple_updates_for_a_single_file() throws Exception
    {
        long coalescingIntervalSeconds = useWatcherWithLongCoalescingInterval();

        Path a = createArtifact("a");

        watch(a);

        // Make sure we override the coalescing interval so that we get events in two coalesced buckets.
        final Instant end = Instant.now().plusSeconds(coalescingIntervalSeconds + 1);
        while (Instant.now().compareTo(end) < 0)
        {
            updateArtifacts(a);
        }
        updateArtifacts(a);
        assertUpdatesAreExactly(a, a);
    }

    @Test
    public void watches_cancelled_during_coalescing_do_not_receive_callbacks() throws Exception
    {
        long coalescingIntervalSeconds = useWatcherWithLongCoalescingInterval();

        Path a = createArtifact("a");

        long id = watch(a).get(0);
        updateArtifacts(a);
        watcher.cancel(id);

        Uninterruptibles.sleepUninterruptibly(coalescingIntervalSeconds + 1, TimeUnit.SECONDS);

        assertThat(updates.poll(coalescingIntervalSeconds + 1, TimeUnit.SECONDS)).isNull();
    }

    @Test
    public void cancelled_files_no_longer_receive_callbacks() throws InterruptedException
    {
        Path a = createArtifact("a");
        Path b = createArtifact("b");

        List<Long> ids = watch(a, b);

        updateArtifacts(a, b);
        assertUpdatesAreExactly(a, b);

        watcher.cancel(ids.get(1));
        updateArtifacts(a, b);
        assertUpdatesAreExactly(a);

        watcher.cancel(ids.get(0));
        updateArtifacts(a, b);
        assertThat(updates.poll(10, TimeUnit.SECONDS)).isNull();
    }

    @Test
    public void watching_a_previously_cancelled_file_works()
    {
        Path a = createArtifact("a");

        long id;

        for (int i = 0; i != 2; ++i)
        {
            id = watch(a).get(0);

            updateArtifacts(a);
            assertUpdatesAreExactly(a);

            watcher.cancel(id);
        }
    }
}
