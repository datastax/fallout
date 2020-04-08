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
package com.datastax.fallout.harness.checkers;

import java.util.Arrays;
import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.EnsembleBuilder;
import com.datastax.fallout.ops.JobConsoleLoggers;
import com.datastax.fallout.ops.NodeGroupBuilder;
import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.ops.configmanagement.FakeConfigurationManager;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;

public class CsvCheckerTest extends TestHelpers.FalloutTest
{
    private Ensemble ensemble()
    {
        NodeGroupBuilder nodeGroupBuilder = NodeGroupBuilder.create()
            .withName("csv-checker-test")
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(new FakeConfigurationManager())
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1)
            .withLoggers(new JobConsoleLoggers())
            .withTestRunArtifactPath(persistentTestOutputDir());

        return EnsembleBuilder.create()
            .withServerGroup(nodeGroupBuilder)
            .withClientGroup(nodeGroupBuilder)
            .withDefaultObserverGroup(nodeGroupBuilder)
            .withDefaultControllerGroup(nodeGroupBuilder)
            .build(persistentTestOutputDir());
    }

    @Test
    public void should_fail_with_empty_histories() throws Exception
    {
        boolean result = new CsvChecker().check(null, Collections.emptyList());
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void should_succeed_with_always_true_sql_query() throws Exception
    {
        String csvContent = "abc,def\n" + //
            "123,456";
        CsvChecker csvChecker = new CsvChecker();
        csvChecker.setProperties(new WritablePropertyGroup(ImmutableMap.of( //
            "fallout.checkers.csv.query", "select true as the_boolean_result from dual;", //
            "fallout.checkers.csv.expect", "TRUE")));
        boolean result = csvChecker.check(ensemble(), Collections.singletonList(
            new Operation("my_process", null, 123L, Operation.Type.ok, MediaType.CSV_UTF_8, csvContent)));
        Assertions.assertThat(result).isTrue();
    }

    @Test
    public void should_fail_when_sql_query_does_not_return_any_row() throws Exception
    {
        String csvContent = "abc,def\n" + //
            "123,456";
        CsvChecker csvChecker = new CsvChecker();
        csvChecker.setProperties(new WritablePropertyGroup(ImmutableMap.of( //
            "fallout.checkers.csv.query", "select abc from my_process where def='789';", //
            "fallout.checkers.csv.expect", "123")));
        boolean result = csvChecker.check(ensemble(), Collections.singletonList(
            new Operation("my_process", null, 123L, Operation.Type.ok, MediaType.CSV_UTF_8, csvContent)));
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void should_fail_when_csv_does_not_satisfy_sql_query() throws Exception
    {
        String csvContent = "abc,def\n" + //
            "12,34\n" +
            "56,78";
        CsvChecker csvChecker = new CsvChecker();
        csvChecker.setProperties(new WritablePropertyGroup(ImmutableMap.of( //
            "fallout.checkers.csv.query", "select abc as abc_expect_12 from my_process where def='78';", //
            "fallout.checkers.csv.expect", "12")));
        boolean result = csvChecker.check(ensemble(), Collections.singletonList(
            new Operation("my_process", null, 123L, Operation.Type.ok, MediaType.CSV_UTF_8, csvContent)));
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void should_ignore_non_csv_operations() throws Exception
    {
        String csvContent = "abc,def\n" + //
            "123,456";
        CsvChecker csvChecker = new CsvChecker();
        csvChecker.setProperties(new WritablePropertyGroup(ImmutableMap.of( //
            "fallout.checkers.csv.query", "select true as the_boolean_result from dual;", //
            "fallout.checkers.csv.expect", "TRUE")));
        boolean result = csvChecker.check(ensemble(), Arrays.asList(
            new Operation("my_process", null, 123L, Operation.Type.ok, MediaType.CSV_UTF_8, csvContent),
            new Operation("my_process", null, 123L, Operation.Type.ok, MediaType.JSON_UTF_8, null)));
        Assertions.assertThat(result).isTrue();
    }

    @Test
    public void should_fail_with_inconsistent_number_of_csv_columns() throws Exception
    {
        String csvContent = "abc,def\n" + //
            "123";
        CsvChecker csvChecker = new CsvChecker();
        csvChecker.setProperties(new WritablePropertyGroup(ImmutableMap.of( //
            "fallout.checkers.csv.query", "select true as the_boolean_result from dual;", //
            "fallout.checkers.csv.expect", "TRUE")));
        boolean result = csvChecker.check(ensemble(), Collections.singletonList(
            new Operation("my_process", null, 123L, Operation.Type.ok, MediaType.CSV_UTF_8, csvContent)));
        Assertions.assertThat(result).isFalse();
    }

    @Test
    public void should_fail_with_too_few_csv_lines() throws Exception
    {
        String csvContent = "abc,def";
        CsvChecker csvChecker = new CsvChecker();
        csvChecker.setProperties(new WritablePropertyGroup(ImmutableMap.of( //
            "fallout.checkers.csv.query", "select true as the_boolean_result from dual;", //
            "fallout.checkers.csv.expect", "TRUE")));
        boolean result = csvChecker.check(ensemble(), Collections.singletonList(
            new Operation("my_process", null, 123L, Operation.Type.ok, MediaType.CSV_UTF_8, csvContent)));
        Assertions.assertThat(result).isFalse();
    }
}
