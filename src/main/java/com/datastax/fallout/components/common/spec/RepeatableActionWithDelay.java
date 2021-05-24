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
package com.datastax.fallout.components.common.spec;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.util.SeededThreadLocalRandom;

public class RepeatableActionWithDelay
{
    private final PropertySpec<Integer> delaySecondsSpec;
    private final PropertySpec<Integer> randomOffsetSecondsSpec;
    private final PropertySpec<Long> seedSpec;
    private final PropertySpec<Integer> numIterationsSpec;
    private int performedIterations = 0;
    private CountDownLatch concurrentModules;
    private SeededThreadLocalRandom random;
    private int numIterations;
    private int delaySeconds;
    private int randomOffsetSeconds;

    public RepeatableActionWithDelay(String prefix)
    {
        delaySecondsSpec = PropertySpecBuilder.createInt(prefix)
            .name("repeat.delay")
            .deprecatedName("sleep.seconds")
            .description("Seconds to wait before each invocation")
            .defaultOf(10)
            .build();
        randomOffsetSecondsSpec = PropertySpecBuilder.createInt(prefix)
            .name("repeat.delay_offset")
            .deprecatedName("sleep.seconds.randomize")
            .description("Maximum extra seconds to wait between invocations. " +
                "repeat.delay is an inclusive lower bound, " +
                "this adds an exclusive upper bound; the total delay is thus a random value between repeat.delay and (repeat.delay + repeat.delay_offset)")
            .defaultOf(1)
            .build();
        numIterationsSpec = PropertySpecBuilder.createInt(prefix)
            .name("repeat.iterations")
            .deprecatedName("iterations")
            .description("Number of times to run this action (0 means repeat during the entire phase. " +
                "if no other modules are in the same phase, 0 thus means run only once)")
            .defaultOf(0)
            .build();
        seedSpec = PropertySpecBuilder.createLong(prefix)
            .name("repeat.seed")
            .description("Set the seed used to initialize the random sequence of values used with repeat.delay_offset")
            .defaultOf(System.nanoTime())
            .build();
    }

    public Collection<PropertySpec<?>> getSpecs()
    {
        return List.of(delaySecondsSpec, randomOffsetSecondsSpec, numIterationsSpec, seedSpec);
    }

    public void init(CountDownLatch concurrentModules, PropertyGroup properties)
    {
        init(new SeededThreadLocalRandom(seedSpec.value(properties)), concurrentModules, properties);
    }

    @VisibleForTesting
    void init(SeededThreadLocalRandom random, CountDownLatch concurrentModules, PropertyGroup properties)
    {
        this.concurrentModules = concurrentModules;
        this.random = random;
        numIterations = numIterationsSpec.value(properties);
        delaySeconds = delaySecondsSpec.value(properties);
        randomOffsetSeconds = randomOffsetSecondsSpec.value(properties);
    }

    private int getDelay()
    {
        return random.nextInt(delaySeconds, delaySeconds + randomOffsetSeconds);
    }

    public int getCurrentIteration()
    {
        return performedIterations;
    }

    private boolean hasNotRun()
    {
        return performedIterations == 0;
    }

    private boolean runUntilModulesFinished()
    {
        return numIterations == 0 && concurrentModules != null && concurrentModules.getCount() != 0;
    }

    private boolean hasRemainingIterations()
    {
        return numIterations != 0 && performedIterations < numIterations;
    }

    public boolean shouldExecute()
    {
        return hasNotRun() || runUntilModulesFinished() || hasRemainingIterations();
    }

    /**
     * Executes <code>runnable</code> if concurrent modules are still executing <i>or<</i> a
     * non-zero value has been given for {@link RepeatableActionWithDelay#numIterationsSpec}.
     * Delays a random number of seconds between {@link RepeatableActionWithDelay#delaySecondsSpec}
     * and {@link RepeatableActionWithDelay#randomOffsetSecondsSpec} before executing.
     *
     * @throws InterruptedException if the Thread is interrupted during the delay
     */
    public void executeDelayed(Runnable runnable) throws InterruptedException
    {
        if (runUntilModulesFinished())
        {
            final var allModulesFinished = concurrentModules.await(getDelay(), TimeUnit.SECONDS);
            if (!allModulesFinished || hasNotRun())
            {
                runnable.run();
                ++performedIterations;
            }
        }
        else
        {
            Thread.sleep(getDelay() * 1000L);
            runnable.run();
            ++performedIterations;
        }
    }
}
