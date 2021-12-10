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
package com.datastax.fallout.components.file_artifact_checkers;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.datastax.fallout.ops.WritablePropertyGroup;
import com.datastax.fallout.test.utils.WithTestResources;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class CsvArtifactCheckerTest extends WithTestResources
{
    private Path testFilesDirectory()
    {
        return getTestClassResourceAsPath(".");
    }

    @Test
    public void should_support_multiple_sql_queries_against_multiple_files() throws Exception
    {
        CsvArtifactChecker checker = new CsvArtifactChecker();
        checker.setProperties(new WritablePropertyGroup(Map.of( //
            "fallout.artifact_checkers.csvchecker.file", "*correct.csv", //
            "fallout.artifact_checkers.csvchecker.separator", ";", //
            "fallout.artifact_checkers.csvchecker.query",
            "select SUM(successes) from (select cast(\"New successes\" as integer) as successes from correct_csv);" +
                "|select SUM(fails) from (select cast(\"New fails\" as integer) as fails from incorrect_csv);", //
            "fallout.artifact_checkers.csvchecker.expect", "3")));
        boolean valid = checker.checkArtifacts(null, testFilesDirectory());
        assertThat(valid).isTrue();
    }

    @Test
    public void should_succeed_when_the_file_exists_and_the_expression_is_satisfied() throws Exception
    {
        CsvArtifactChecker checker = new CsvArtifactChecker();
        checker.setProperties(new WritablePropertyGroup(Map.of( //
            "fallout.artifact_checkers.csvchecker.file", "correct.csv", //
            "fallout.artifact_checkers.csvchecker.separator", ";", //
            "fallout.artifact_checkers.csvchecker.query",
            "select SUM(theCast) from (select cast(\"New successes\" as integer) as theCast from correct_csv);", //
            "fallout.artifact_checkers.csvchecker.expect", "3")));
        boolean valid = checker.checkArtifacts(null, testFilesDirectory());
        assertThat(valid).isTrue();
    }

    @Test
    public void should_convert_dots_and_dashes_to_underscores_in_file_names() throws Exception
    {
        CsvArtifactChecker checker = new CsvArtifactChecker();
        checker.setProperties(new WritablePropertyGroup(Map.of( //
            "fallout.artifact_checkers.csvchecker.file", "abs.olute-ly.correct.csv", //
            "fallout.artifact_checkers.csvchecker.separator", ";", //
            "fallout.artifact_checkers.csvchecker.query",
            "select SUM(theCast) from (select cast(\"New successes\" as integer) as theCast from abs_olute_ly_correct_csv);", //
            "fallout.artifact_checkers.csvchecker.expect", "6")));
        boolean valid = checker.checkArtifacts(null, testFilesDirectory());
        assertThat(valid).isTrue();
    }

    @Test
    public void should_fail_when_the_expression_is_not_satisfied() throws Exception
    {
        CsvArtifactChecker checker = new CsvArtifactChecker();
        checker.setProperties(new WritablePropertyGroup(Map.of( //
            "fallout.artifact_checkers.csvchecker.file", "incorrect.csv",
            "fallout.artifact_checkers.csvchecker.separator", ";", //
            "fallout.artifact_checkers.csvchecker.query",
            "select SUM(theCast) from (select cast(\"New fails\" as integer) as theCast from incorrect_csv);", //
            "fallout.artifact_checkers.csvchecker.expect", "0")));
        boolean valid = checker.checkArtifacts(null, testFilesDirectory());
        assertThat(valid).isFalse();
    }

    @Test
    public void should_fail_when_no_file_matches_the_filter() throws Exception
    {
        CsvArtifactChecker checker = new CsvArtifactChecker();
        checker.setProperties(new WritablePropertyGroup(Map.of( //
            "fallout.artifact_checkers.csvchecker.file", "unknown.csv",
            "fallout.artifact_checkers.csvchecker.separator", ";", //
            "fallout.artifact_checkers.csvchecker.query",
            "select SUM(theCast) from (select cast(\"New fails\" as integer) as theCast from unknown_csv);", //
            "fallout.artifact_checkers.csvchecker.expect", "0")));
        boolean valid = checker.checkArtifacts(null, testFilesDirectory());
        assertThat(valid).isFalse();
    }
}
