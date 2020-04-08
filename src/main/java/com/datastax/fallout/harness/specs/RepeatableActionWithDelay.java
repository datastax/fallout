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
package com.datastax.fallout.harness.specs;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.util.SeededThreadLocalRandom;

public class RepeatableActionWithDelay
{
    private final PropertySpec<Integer> delaySeconds;
    private final PropertySpec<Integer> randomOffsetSeconds;
    private final PropertySpec<Long> seed;
    private final PropertySpec<Integer> numIterations;
    private final AtomicInteger performedIterations = new AtomicInteger();
    private SeededThreadLocalRandom random;

    public RepeatableActionWithDelay(String prefix)
    {
        delaySeconds = PropertySpecBuilder.createInt(prefix)
            .name("repeat.delay")
            .deprecatedName("sleep.seconds")
            .description("Seconds to wait before each invocation")
            .defaultOf(10)
            .build();
        randomOffsetSeconds = PropertySpecBuilder.createInt(prefix)
            .name("repeat.delay_offset")
            .deprecatedName("sleep.seconds.randomize")
            .description("Maximum extra seconds to wait between invocations. " +
                "repeat.delay is an inclusive lower bound, " +
                "this adds an exclusive upper bound; the total delay is thus a random value between repeat.delay and (repeat.delay + repeat.delay_offset)")
            .defaultOf(1)
            .build();
        numIterations = PropertySpecBuilder.createInt(prefix)
            .name("repeat.iterations")
            .deprecatedName("iterations")
            .description("Number of times to run this action (0 means repeat during the entire phase)")
            .defaultOf(0)
            .build();
        seed = PropertySpecBuilder.createLong(prefix)
            .name("repeat.seed")
            .description("Set the seed used to initialize the random sequence of values used with repeat.delay_offset")
            .defaultOf(System.nanoTime())
            .build();
    }

    public Collection<PropertySpec<?>> getSpecs()
    {
        return Arrays.asList(delaySeconds, randomOffsetSeconds, numIterations, seed);
    }

    public void init(PropertyGroup properties)
    {
        init(new SeededThreadLocalRandom(seed.value(properties)));
    }

    @VisibleForTesting
    public void init(SeededThreadLocalRandom random)
    {
        this.random = random;
    }

    public boolean canRunNewIteration(PropertyGroup properties, CountDownLatch concurrentModules)
    {
        final int numIterations = this.numIterations.value(properties);
        final int performedIterations = this.performedIterations.get();

        final boolean hasNotRun = performedIterations == 0;
        final boolean runForever = concurrentModules != null && numIterations == 0;
        final boolean hasRemainingIterations = performedIterations < numIterations;

        return hasNotRun || runForever || hasRemainingIterations;
    }

    /**
     * Executes a given task if concurrent modules are still executing and after a random number of seconds between
     * {@link RepeatableActionWithDelay#delaySeconds} and {@link RepeatableActionWithDelay#randomOffsetSeconds} is elapsed.
     *
     * @param runnable          the task to execute
     * @param concurrentModules the number of concurrent modules that are executing
     * @param properties        a {@link PropertyGroup} that may contain values for {@code delaySeconds} and {@code randomOffsetSeconds}
     * @return {@code true} if the task was executed, {@code false} otherwise
     * @throws InterruptedException if the Thread is interrupted during the delay
     */
    public boolean executeDelayed(Runnable runnable, CountDownLatch concurrentModules, PropertyGroup properties)
        throws InterruptedException
    {
        int performedIterations = this.performedIterations.incrementAndGet();

        final boolean hasNotRun = performedIterations == 1;
        final boolean execute = !concurrentModulesFinishedBeforeDelay(concurrentModules, properties) || hasNotRun;

        if (execute)
        {
            runnable.run();
        }

        return execute;
    }

    /**
     * Waits until one of the two conditions are satisfied:
     * <ul>
     * <li>{@code otherModules} reaches zero</li>
     * <li>a random number of seconds between {@link RepeatableActionWithDelay#delaySeconds} and {@link
     * RepeatableActionWithDelay#randomOffsetSeconds} is elapsed</li>
     * </ul>
     *
     * @param otherModules a {@link CountDownLatch} representing how many modules are concurrently executing
     * @param properties   a {@link PropertyGroup} that may contain values for {@code delaySeconds} and {@code randomOffsetSeconds}
     * @return {@code true} if {@code otherModules} reached zero before the delay was elapsed, {@code false} otherwise
     */
    private boolean concurrentModulesFinishedBeforeDelay(CountDownLatch otherModules, PropertyGroup properties)
        throws InterruptedException
    {
        int delay = random.nextInt(delaySeconds.value(properties),
            delaySeconds.value(properties) + randomOffsetSeconds.value(properties));

        if (otherModules != null)
        {
            return otherModules.await(delay, TimeUnit.SECONDS);
        }
        else
        {
            Thread.sleep(delay * 1000);
            return false;
        }
    }

    public int getCurrentIteration()
    {
        return performedIterations.get();
    }
}
