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
package com.datastax.fallout.service.artifacts;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableMap;
import com.sun.nio.file.SensitivityWatchEventModifier;
import io.dropwizard.lifecycle.Managed;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ExecutorServices;
import com.datastax.fallout.util.NamedThreadFactory;
import com.datastax.fallout.util.ScopedLogger;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class ArtifactWatcher implements Managed
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(ArtifactWatcher.class);

    private final Path artifactRoot;
    private final HashedWheelTimer eventCoalescingTimer;
    private final int coalescingIntervalSeconds;

    private Watcher watcher;
    private CoalescedPathEvents coalescedPathEvents;
    private ExecutorService executor =
        Executors.newSingleThreadExecutor(new NamedThreadFactory("ArtifactWatcher"));
    private AtomicBoolean running = new AtomicBoolean();

    private static class WatchedDir
    {
        final WatchKey watchKey;
        final Map<Path, Map<Long, Consumer<Path>>> callbacksForFiles;

        private WatchedDir(WatchKey watchKey)
        {
            this.watchKey = watchKey;
            this.callbacksForFiles = new HashMap<>();
        }
    }

    private class CoalescedPathEvents implements Closeable
    {
        private final Set<Path> coalescedPathEvents = new HashSet<>();
        Optional<Timeout> timeout = Optional.empty();

        public synchronized void addPathEvent(Path path)
        {
            coalescedPathEvents.add(path);
            if (!timeout.isPresent())
            {
                timeout = Optional.of(eventCoalescingTimer.newTimeout(this::processPathEvents,
                    coalescingIntervalSeconds, TimeUnit.SECONDS));
            }
        }

        private synchronized void processPathEvents(Timeout ignored)
        {
            timeout = Optional.empty();
            coalescedPathEvents.forEach(ArtifactWatcher.this::runCallbacksForFile);
            coalescedPathEvents.clear();
        }

        @Override
        public synchronized void close()
        {
            timeout.ifPresent(Timeout::cancel);
        }
    }

    private static class Watcher implements Closeable
    {
        private final WatchService watcher;
        private final Map<Path, WatchedDir> watchedDirs;
        private final Map<Long, Path> absoluteArtifactPathsByWatchId;

        private Watcher() throws IOException
        {
            watcher = FileSystems.getDefault().newWatchService();
            watchedDirs = new HashMap<>();
            absoluteArtifactPathsByWatchId = new HashMap<>();
        }

        @Override
        public void close() throws IOException
        {
            watcher.close();
        }

        synchronized Optional<Map<Long, Consumer<Path>>> callbacksForFile(Path absoluteArtifactPath)
        {
            return Optional
                .ofNullable(watchedDirs.get(absoluteArtifactPath.getParent()))
                .flatMap(watchedDir -> Optional.ofNullable(
                    watchedDir.callbacksForFiles.get(absoluteArtifactPath.getFileName())))
                // Take a copy, as it will be read outside of synchronisation,
                // and we want to avoid ConcurrentModificationExceptions.
                .map(ImmutableMap::copyOf);
        }

        synchronized long watch(Path absoluteArtifactPath, Consumer<Path> modifiedCallback)
        {
            final Path absoluteArtifactDirPath = absoluteArtifactPath.getParent();

            final WatchedDir watchedDir =
                watchedDirs.computeIfAbsent(absoluteArtifactDirPath,
                    ignored -> new WatchedDir(Exceptions.getUnchecked(() -> {
                        logger.info("Adding dir watch for {}", absoluteArtifactDirPath);
                        return absoluteArtifactDirPath.register(watcher,
                            new WatchEvent.Kind[] {ENTRY_MODIFY},
                            /* The following only has any effect on systems without a native filesystem notification
                             * system i.e. those that have to use polling; currently this includes MacOS (as of
                             * Java 1.8), but not Linux. */
                            SensitivityWatchEventModifier.HIGH);
                    })));

            final Map<Long, Consumer<Path>> callbacksForFile = watchedDir.callbacksForFiles
                .computeIfAbsent(absoluteArtifactPath.getFileName(), filePath -> new HashMap<>());

            final long watchId = absoluteArtifactPathsByWatchId.keySet().stream()
                .mapToLong(l -> l)
                .max()
                .orElse(0L) + 1;

            logger.info("Adding file watch {} for {}", watchId, absoluteArtifactPath);

            callbacksForFile.put(watchId, modifiedCallback);
            absoluteArtifactPathsByWatchId.put(watchId, absoluteArtifactPath);

            return watchId;
        }

        synchronized void cancel(long watchId)
        {
            final Path absoluteArtifactPath = absoluteArtifactPathsByWatchId.remove(watchId);

            if (absoluteArtifactPath == null)
            {
                return;
            }

            final Path absoluteArtifactDirPath = absoluteArtifactPath.getParent();

            final WatchedDir watchedDir = watchedDirs.get(absoluteArtifactDirPath);
            if (watchedDir == null)
            {
                return;
            }

            final Path filePath = absoluteArtifactPath.getFileName();

            logger.info("Cancelling file watch {} for {}", watchId, absoluteArtifactPath);

            final Map<Long, Consumer<Path>> callbacks = watchedDir.callbacksForFiles.get(filePath);
            callbacks.remove(watchId);

            if (callbacks.isEmpty())
            {
                watchedDir.callbacksForFiles.remove(filePath);
            }

            if (watchedDir.callbacksForFiles.isEmpty())
            {
                logger.info("Cancelling dir watch for {}", (Path) watchedDir.watchKey.watchable());
                watchedDir.watchKey.cancel();
                watchedDirs.remove(absoluteArtifactDirPath);
            }
        }

        WatchKey poll()
        {
            try
            {
                return watcher.poll(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e)
            {
                return null;
            }
        }
    }

    public ArtifactWatcher(Path artifactRoot, HashedWheelTimer eventCoalescingTimer,
        int coalescingIntervalSeconds)
    {
        this.artifactRoot = artifactRoot.toAbsolutePath();
        this.eventCoalescingTimer = eventCoalescingTimer;
        this.coalescingIntervalSeconds = coalescingIntervalSeconds;
    }

    public Path getAbsolutePath(Path relativeArtifactPath)
    {
        return artifactRoot.resolve(relativeArtifactPath);
    }

    @Override
    public void start() throws Exception
    {
        watcher = new Watcher();
        coalescedPathEvents = new CoalescedPathEvents();
        executor.execute(this::run);
    }

    private void run()
    {
        logger.info("Starting polling loop");
        running.set(true);
        while (running.get())
        {
            poll();
        }
        logger.info("Stopping polling loop");
    }

    @Override
    public void stop() throws Exception
    {
        while (!running.getAndSet(false))
            ;

        // If we use shutdown(), WatchKey.poll() may block up to 5 seconds before run() detects that we need to
        // stop.  shutdownNow() terminates WatchKey.poll() early with an InterruptedException, which we
        // handle: this allows run() to detect the stop request earlier, and makes this method faster.
        ExecutorServices.shutdownNowAndAwaitTermination(logger, executor, "ArtifactWatcher");
        watcher.close();
        coalescedPathEvents.close();
    }

    private Path absoluteArtifactPath(Path relativeArtifactPath)
    {
        return artifactRoot.resolve(relativeArtifactPath).toAbsolutePath();
    }

    private Path relativeArtifactPath(Path absoluteArtifactPath)
    {
        return artifactRoot.relativize(absoluteArtifactPath);
    }

    public long watch(Path relativeArtifactPath, Consumer<Path> modifiedCallback)
    {
        return watcher.watch(absoluteArtifactPath(relativeArtifactPath), modifiedCallback);
    }

    private void runCallbacksForFile(Path absoluteArtifactPath)
    {
        watcher.callbacksForFile(absoluteArtifactPath)
            .ifPresent(callbacks -> {
                logger.debug("running callbacks {} for {}", callbacks.keySet(), absoluteArtifactPath);
                callbacks.values().stream()
                    .forEach(callback -> callback.accept(relativeArtifactPath(absoluteArtifactPath)));
            });
    }

    public void cancel(long watchId)
    {
        watcher.cancel(watchId);
    }

    private void poll()
    {
        WatchKey key = watcher.poll();
        if (key == null)
        {
            return;
        }

        Path dirPath = (Path) key.watchable();

        key.pollEvents().stream()
            .filter(event -> event.kind() == ENTRY_MODIFY)
            .map(event -> (WatchEvent<Path>) event)
            .forEach(event -> coalescedPathEvents.addPathEvent(dirPath.resolve(event.context())));

        key.reset();
    }
}
