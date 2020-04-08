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
package com.datastax.fallout.harness.modules;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

import com.google.common.collect.Range;
import org.junit.Test;

import static com.datastax.fallout.harness.modules.nosqlbench.NoSqlBenchModule.distributeCycleRangeByClient;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;

public class NoSqlBenchCycleRangeTest
{
    private static class Fixture
    {
        long totalCycles;
        long cycleOffset;
        int numClients;
    }

    private void assertCycleRanges(BiConsumer<Fixture, Stream<Range<Long>>> assertion)
    {
        qt()
            .forAll(
                longs().between(1L, 10000000000L),
                longs().between(0L, 10000000000L),
                integers().between(1, 2000))
            .checkAssert((totalCycles_, cycleOffset_, numClients_) -> assertion.accept(new Fixture()
            {
                {
                    totalCycles = totalCycles_;
                    cycleOffset = cycleOffset_;
                    numClients = numClients_;
                }
            }, distributeCycleRangeByClient(totalCycles_, cycleOffset_, numClients_)));
    }

    @Test
    public void the_number_of_produced_cycle_range_arguments_is_equal_to_the_number_of_clients()
    {
        assertCycleRanges((fixture, cycleRanges) -> assertThat(cycleRanges.count()).isEqualTo(fixture.numClients));
    }

    @Test
    public void the_accumulated_cycle_range_for_each_client_is_equal_to_the_total_number_of_cycles()
    {
        assertCycleRanges((fixture, cycleRanges) -> {
            long totalAccumulatedCycles = cycleRanges
                .mapToLong(r -> r.upperEndpoint() - r.lowerEndpoint())
                .sum();
            assertThat(totalAccumulatedCycles).isEqualTo(fixture.totalCycles);
        });
    }

    @Test
    public void the_first_cycle_given_to_the_first_client_is_equal_to_the_cycle_offset()
    {
        assertCycleRanges((fixture, cycleRanges) -> {
            assertThat(cycleRanges.findFirst().map(Range::lowerEndpoint)).hasValue(fixture.cycleOffset);
        });
    }

    @Test
    public void the_last_cycle_given_to_the_last_client_is_equal_to_the_total_cycles_plus_offset()
    {
        assertCycleRanges((fixture, cycleRanges) -> {
            assertThat(cycleRanges.reduce((a, b) -> b).map(Range::upperEndpoint))
                .hasValue(fixture.totalCycles + fixture.cycleOffset);
        });
    }

    @Test
    public void the_end_of_one_cycle_range_is_equal_to_the_start_of_the_next_cycle_range_in_the_sequence()
    {
        assertCycleRanges((fixture, cycleRanges) -> {
            cycleRanges.reduce((prev, cur) -> {
                assertThat(prev.upperEndpoint()).isEqualTo(cur.lowerEndpoint());
                return cur;
            });
        });
    }
}
