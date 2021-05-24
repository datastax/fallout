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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.datastax.fallout.runner.TestRunStatusUpdate;

/** Handles synchronous publication of TestRunStatusUpdate events.
 *  Subscribers are notified in order of subscribe calls. */
public class TestRunStatusUpdatePublisher
{
    private List<Subscription> subscriptions = new ArrayList<>();

    public class Subscription
    {
        private final Predicate<TestRunStatusUpdate> subscriber;
        boolean cancelled = false;

        private Subscription(Predicate<TestRunStatusUpdate> subscriber)
        {
            this.subscriber = subscriber;
        }

        /** Cancel a subscription.  This can safely be called multiple times. */
        public void cancel()
        {
            if (!cancelled)
            {
                TestRunStatusUpdatePublisher.this.cancel(this);
            }
            cancelled = true;
        }
    }

    public synchronized Subscription subscribe(Predicate<TestRunStatusUpdate> subscriber)
    {
        final var subscription = new Subscription(subscriber);
        subscriptions.add(subscription);
        return subscription;
    }

    private synchronized void cancel(Subscription subscription)
    {
        final var wasRemoved = subscriptions.remove(subscription);
        assert wasRemoved;
    }

    /** Publish update, returning true only if all subscribers returned true */
    public synchronized boolean publish(TestRunStatusUpdate update)
    {
        // Take a copy of the list to avoid concurrent modification by subscribers that cancel or add
        // another subscription
        return List.copyOf(this.subscriptions).stream()
            .map(subscription -> subscription.subscriber.test(update))
            .reduce(Boolean::logicalAnd)
            .orElse(true);
    }
}
