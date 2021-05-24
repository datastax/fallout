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

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.util.SeededThreadLocalRandom;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.components.common.spec.RepeatableActionWithDelayTest.ConcurrentModules.SOME_MODULES_UNFINISHED;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
@Timeout(5)
public class RepeatableActionWithDelayTest
{
    private static final String PREFIX = "my.prefix.";

    private RepeatableActionWithDelay repeatableActionWithDelay;
    private CountDownLatch unfinishedModules;

    @Mock
    public SeededThreadLocalRandom random;

    @Mock
    public Runnable target;

    @BeforeEach
    public void setUp()
    {
        repeatableActionWithDelay = new RepeatableActionWithDelay(PREFIX);
    }

    enum ConcurrentModules
    {
        NO_MODULES(-1),
        ALL_MODULES_FINISHED(0),
        SOME_MODULES_UNFINISHED(1);

        private final int count;

        ConcurrentModules(int count)
        {
            this.count = count;
        }

        CountDownLatch createCountDownLatch()
        {
            return count < 0 ? null : new CountDownLatch(count);
        }
    }

    private void givenConcurrentModulesAndProperties(CountDownLatch unfinishedModules,
        Map<String, Integer> propertyValues)
    {
        this.unfinishedModules = unfinishedModules;

        final var properties = new WritablePropertyGroup().with(PREFIX);
        propertyValues.forEach((key, value) -> properties.put("repeat." + key, value));
        repeatableActionWithDelay.init(random, unfinishedModules, properties);
    }

    private void givenConcurrentModulesAndProperties(ConcurrentModules concurrentModules,
        Map<String, Integer> propertyValues)
    {
        givenConcurrentModulesAndProperties(concurrentModules.createCountDownLatch(), propertyValues);
    }

    private void whenAllExecutionsArePerformed() throws InterruptedException
    {
        while (repeatableActionWithDelay.shouldExecute())
        {
            repeatableActionWithDelay.executeDelayed(target);
        }
    }

    private void whenSomeExecutionsArePerformed(int count) throws InterruptedException
    {
        for (int i = 0; i < count; ++i)
        {
            assertThat(repeatableActionWithDelay.shouldExecute()).isTrue();
            repeatableActionWithDelay.executeDelayed(target);
        }
    }

    @Test
    public void repeats_indefinitely_with_iterations_equal_to_zero_while_other_modules_are_unfinished() throws Exception
    {
        givenConcurrentModulesAndProperties(SOME_MODULES_UNFINISHED, Map.of(
            "iterations", 0,
            "delay", 0,
            "delay_offset", 0));

        whenSomeExecutionsArePerformed(5);

        then(target).should(times(5)).run();

        unfinishedModules.countDown();

        whenAllExecutionsArePerformed();

        then(target).shouldHaveNoMoreInteractions();
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(value = ConcurrentModules.class, names = {"ALL_MODULES_FINISHED", "NO_MODULES"})
    public void executes_once_if_other_modules_are_finished_or_not_present(ConcurrentModules concurrentModules)
        throws Exception
    {
        givenConcurrentModulesAndProperties(concurrentModules, Map.of(
            "iterations", 0,
            "delay", 0,
            "delay_offset", 0));

        whenAllExecutionsArePerformed();

        then(target).should(times(1)).run();
    }

    @ParameterizedTest(name = "{displayName} [{0}]")
    @EnumSource(value = ConcurrentModules.class)
    public void executes_exactly_iterations_times_if_iterations_are_non_zero(ConcurrentModules concurrentModules)
        throws Exception
    {
        final var iterations = 7;

        givenConcurrentModulesAndProperties(concurrentModules, Map.of(
            "iterations", iterations,
            "delay", 0,
            "delay_offset", 0));

        whenAllExecutionsArePerformed();

        then(target).should(times(iterations)).run();
    }

    @Test
    public void delays_before_executing_if_other_modules_are_unfinished() throws InterruptedException
    {
        givenConcurrentModulesAndProperties(SOME_MODULES_UNFINISHED, Map.of(
            "iterations", 0,
            "delay", 0,
            "delay_offset", 0));

        assertThat(repeatableActionWithDelay.shouldExecute()).isTrue();

        final int delayInSeconds = 1;
        given(random.nextInt(anyInt(), anyInt())).willReturn(delayInSeconds);

        final Instant start = Instant.now();
        AtomicReference<Instant> end = new AtomicReference<>(Instant.now());

        repeatableActionWithDelay.executeDelayed(() -> end.set(Instant.now()));

        assertThat(end.get())
            .isAfterOrEqualTo(start.plusSeconds(delayInSeconds));
    }

    @Test
    @Timeout(value = 1500, unit = TimeUnit.MILLISECONDS)
    public void picks_a_random_amount_of_seconds_between_min_and_max_for_delay() throws Exception
    {
        final int delay = 1;
        final int delayOffset = 71;
        final int iterations = 10;

        givenConcurrentModulesAndProperties(SOME_MODULES_UNFINISHED, Map.of(
            "iterations", iterations,
            "delay", delay,
            "delay_offset", delayOffset));

        whenAllExecutionsArePerformed();

        then(target).should(times(iterations)).run();
        then(random).should(times(iterations)).nextInt(delay, delay + delayOffset);
    }
}
