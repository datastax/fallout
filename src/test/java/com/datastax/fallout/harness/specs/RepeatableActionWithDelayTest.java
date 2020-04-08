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

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.util.SeededThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

public class RepeatableActionWithDelayTest
{
    private static final String PREFIX = "my.prefix.";

    private RepeatableActionWithDelay repeatableActionWithDelay;
    private WritablePropertyGroup properties;

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

    @Mock
    public SeededThreadLocalRandom random;

    @Mock
    public Runnable target;

    @Before
    public void setUp() throws Exception
    {
        repeatableActionWithDelay = new RepeatableActionWithDelay(PREFIX);
        repeatableActionWithDelay.init(random);
        properties = new WritablePropertyGroup().with(PREFIX);
    }

    @Test
    public void repeats_indefinitely_with_iterations_equal_to_zero() throws Exception
    {
        properties.put("repeat.iterations", 0);

        final CountDownLatch unfinishedModules = new CountDownLatch(1);
        final CountDownLatch concurrentModules = new CountDownLatch(0);
        for (int i = 0; i < 3; i++)
        {
            assertThat(repeatableActionWithDelay.canRunNewIteration(properties, unfinishedModules)).isTrue();
            assertThat(repeatableActionWithDelay.executeDelayed(() -> {}, unfinishedModules, properties)).isTrue();
        }
    }

    @Test
    public void executes_once_if_count_down_latch_is_zero() throws Exception
    {
        properties.put("repeat.iterations", 0);

        CountDownLatch unfinishedModules = new CountDownLatch(0);

        assertThat(repeatableActionWithDelay.canRunNewIteration(properties, unfinishedModules)).isTrue();
        assertThat(repeatableActionWithDelay.executeDelayed(target, unfinishedModules, properties)).isTrue();
        for (int i = 0; i < 10; i++)
        {
            assertThat(repeatableActionWithDelay.executeDelayed(target, unfinishedModules, properties)).isFalse();
        }

        verify(target, Mockito.times(1)).run();
    }

    @Test
    public void delays_before_executing_if_other_modules_are_unfinished() throws InterruptedException
    {
        properties.put("repeat.iterations", 0);
        CountDownLatch unfinishedModules = new CountDownLatch(1);

        assertThat(repeatableActionWithDelay.canRunNewIteration(properties, unfinishedModules)).isTrue();

        final int delayInSeconds = 1;
        given(random.nextInt(anyInt(), anyInt())).willReturn(delayInSeconds);

        final Instant start = Instant.now();
        AtomicReference<Instant> end = new AtomicReference<>(Instant.now());

        assertThat(repeatableActionWithDelay.executeDelayed(
            () -> end.set(Instant.now()), unfinishedModules, properties)).isTrue();

        assertThat(end.get())
            .isAfterOrEqualTo(start.plusSeconds(delayInSeconds));
    }

    @Test(timeout = 1500)
    public void picks_a_random_amount_of_seconds_between_min_and_max_for_delay() throws Exception
    {
        final int delay = 1;
        final int delayOffset = 71;
        final int iterations = 10;

        properties
            .put("repeat.iterations", iterations)
            .put("repeat.delay", delay)
            .put("repeat.delay_offset", delayOffset);

        CountDownLatch unfinishedModules = new CountDownLatch(1);
        for (int i = 0; i < iterations; i++)
        {
            assertThat(repeatableActionWithDelay.canRunNewIteration(properties, unfinishedModules)).isTrue();
            assertThat(repeatableActionWithDelay.executeDelayed(target, unfinishedModules, properties)).isTrue();
        }

        assertThat(repeatableActionWithDelay.canRunNewIteration(properties, unfinishedModules)).isFalse();

        verify(target, Mockito.times(iterations)).run();
        verify(random, Mockito.times(iterations)).nextInt(delay, delay + delayOffset);
    }
}
