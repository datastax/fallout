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
package com.datastax.fallout.service.db;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import static com.codahale.metrics.MetricRegistry.name;

public class QueueMetricsManager
{
    private static final Duration LONGEST_TEST_RUN_FINISHED_DURATION = Duration.of(24, ChronoUnit.HOURS);
    private static final long SUPPLIER_EXPIRATION_SECONDS = 2;

    private static class SupplierHistogram extends Histogram
    {
        private Supplier<Collection<Long>> supplier;

        public SupplierHistogram(Supplier<Collection<Long>> supplier)
        {
            super(new Reservoir() {
                @Override
                public int size()
                {
                    return 0;
                }

                @Override
                public void update(long value)
                {
                    //nothing here
                }

                @Override
                public Snapshot getSnapshot()
                {
                    return new UniformSnapshot(supplier.get());
                }
            });

            this.supplier = supplier;
        }

        @Override
        public long getCount()
        {
            return supplier.get().size();
        }
    }

    public static void registerMetrics(MetricRegistry metricRegistry, TestRunDAO testRunDAO)
    {
        SupplierHistogram queuedTestRunsTimeOnQueueStats = new SupplierHistogram(Suppliers.memoizeWithExpiration(() -> {
            if (!testRunDAO.isRunning())
                return List.of();

            final var now = Instant.now();

            return testRunDAO
                .getQueued().stream()
                .map(testRun -> Duration.between(testRun.getCreatedAt().toInstant(), now).toMillis())
                .toList();
        },
            SUPPLIER_EXPIRATION_SECONDS, TimeUnit.SECONDS)
        );

        SupplierHistogram recentFinishedTestRunsExecutionTimeStats =
            new SupplierHistogram(Suppliers.memoizeWithExpiration(() -> {
                if (!testRunDAO.isRunning())
                    return List.of();

                final var now = Instant.now();

                return testRunDAO
                    .getAllFinishedTestRunsThatFinishedBetweenInclusive(now.minus(LONGEST_TEST_RUN_FINISHED_DURATION),
                        now)
                    .map(testRun -> Duration.between(testRun.getStartedAt().toInstant(), now).toMillis())
                    .toList();
            },
                SUPPLIER_EXPIRATION_SECONDS, TimeUnit.SECONDS)
            );

        metricRegistry.register(name(QueueMetricsManager.class, "time-on-queue"), queuedTestRunsTimeOnQueueStats);
        metricRegistry.register(name(QueueMetricsManager.class, "execution-time"),
            recentFinishedTestRunsExecutionTimeStats);
    }

}
