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
package com.datastax.fallout.util;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import com.sun.nio.file.SensitivityWatchEventModifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static org.assertj.core.api.Assertions.assertThat;

/** Check basic behaviour of WatchService, since different platforms have different implementations. */
public class WatchServiceTest extends TestHelpers.ArtifactTest
{
    private WatchService watcher;
    private Path watched;

    @Before
    public void setup() throws IOException
    {
        watcher = FileSystems.getDefault().newWatchService();
        watched = createTestRunArtifact("foo.log", "Hello\n");
        watched.getParent().register(watcher, new WatchEvent.Kind[] {ENTRY_MODIFY}, SensitivityWatchEventModifier.HIGH);
    }

    @After
    public void teardown() throws IOException
    {
        watcher.close();
    }

    private WatchKey poll()
    {
        return Exceptions.getUnchecked(() -> watcher.poll(
            // The SensitivityWatchEventModifier only applies to platforms where a non-native
            // (i.e. polling) implementation of WatchService is used.  We need to ensure we poll
            // for longer than the polling rate in WatchService, so we don't miss any changes.
            SensitivityWatchEventModifier.HIGH.sensitivityValueInSeconds() + 2, TimeUnit.SECONDS));
    }

    private void updateArtifact()
    {
        updateTestRunArtifact(watched, "Again\n");
    }

    @Test
    public void file_changes_are_picked_up_between_polls()
    {
        WatchKey key = poll();

        assertThat(key).isNull();

        updateArtifact();

        key = poll();

        assertThat(key).isNotNull();
    }

    @Test
    public void file_changes_are_picked_up_during_polls() throws InterruptedException
    {
        WatchKey key = poll();

        assertThat(key).isNull();

        CountDownLatch pollStarting = new CountDownLatch(1);

        CompletableFuture<WatchKey> keyFuture = CompletableFuture.supplyAsync(() -> Exceptions.getUnchecked(() -> {
            pollStarting.countDown();
            return poll();
        }));

        pollStarting.await();
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        updateArtifact();

        assertThat(keyFuture.join()).isNotNull();
    }
}
