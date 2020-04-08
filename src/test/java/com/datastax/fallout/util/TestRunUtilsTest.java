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
package com.datastax.fallout.util;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.harness.TestRunnerTestHelpers.makeTest;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRunUtilsTest extends TestHelpers.FalloutTest
{
    private final String PREFIX_1 = "test 11111111 2018-01-01 16:40:00";
    private final String PREFIX_2 = "test 22222222 2018-02-02 16:40:00";
    private final String PREFIX_3 = "test 33333333 2018-03-03 16:40:00";
    com.datastax.fallout.service.core.Test test;
    TestRun run1;
    TestRun run2;
    TestRun run3;
    List<TestRun> testRuns;

    @Before
    public void setUp()
    {
        List<String> defHeaderLines = new ArrayList<>();
        defHeaderLines.add("yaml_param: 1");
        defHeaderLines.add("---");

        test = makeTest(getTestUser().getEmail(), "fakes.yaml");
        test.setDefinition(String.join("\n", defHeaderLines) + "\n" + test.getDefinition());
        test.setName("test");

        run1 = test.createTestRun(Map.of());
        run1.setTestRunId(UUID.fromString("11111111-8A91-4ABB-A2C8-B669189FEFD5"));
        run1.setStartedAt(Date.from(Instant.parse("2018-01-01T16:40:00Z")));

        run2 = test.createTestRun(Map.of());
        run2.setTestRunId(UUID.fromString("22222222-8A91-4ABB-A2C8-B669189FEFD5"));
        run2.setStartedAt(Date.from(Instant.parse("2018-02-02T16:40:00Z")));

        run3 = test.createTestRun(Map.of());
        run3.setTestRunId(UUID.fromString("33333333-8A91-4ABB-A2C8-B669189FEFD5"));
        run3.setStartedAt(Date.from(Instant.parse("2018-03-03T16:40:00Z")));

        testRuns = new ArrayList<>();
        testRuns.add(run1);
        testRuns.add(run2);
    }

    private void assertDisplayName(TestRun run, String expectedName)
    {
        Map<TestRun, String> displayNames = TestRunUtils.buildTestRunDisplayNames(testRuns);
        assertThat(displayNames.get(run)).isEqualTo(expectedName);
    }

    private void assertDefaultDisplayNames()
    {
        assertDisplayName(run1, PREFIX_1);
        assertDisplayName(run2, PREFIX_2);
    }

    @Test
    public void defaults_are_as_expected()
    {
        assertDefaultDisplayNames();
    }

    @Test
    public void defaults_when_same_param_value()
    {
        run1.setTemplateParamsMap(Map.of("bbb", "same"));
        run2.setTemplateParamsMap(Map.of("bbb", "same"));
        assertDefaultDisplayNames();
    }

    @Test
    public void always_displayed_param_even_with_same_value()
    {
        run1.setTemplateParamsMap(Map.of("dse_version", "5.1"));
        run2.setTemplateParamsMap(Map.of("dse_version", "5.1"));

        assertDisplayName(run1, PREFIX_1 + " dse_version: 5.1");
        assertDisplayName(run2, PREFIX_2 + " dse_version: 5.1");
    }

    @Test
    public void different_param_value()
    {
        run1.setTemplateParamsMap(Map.of("aaa", 1));
        run2.setTemplateParamsMap(Map.of("aaa", 2));

        assertDisplayName(run1, PREFIX_1 + " aaa: 1");
        assertDisplayName(run2, PREFIX_2 + " aaa: 2");

        run2.setTestName("otherTest");

        assertDisplayName(run1, PREFIX_1);
        assertDisplayName(run2, PREFIX_2.replace(test.getName(), "otherTest"));
    }

    @Test
    public void yaml_defaults_are_considered()
    {
        run1.setTemplateParamsMap(Map.of("bbb", "same"));
        run2.setTemplateParamsMap(Map.of(
            "bbb", "same",
            "yaml_param", 2
        ));

        assertDisplayName(run1, PREFIX_1 + " yaml_param: 1");
        assertDisplayName(run2, PREFIX_2 + " yaml_param: 2");
    }

    @Test
    public void third_testrun_without_params()
    {
        run1.setTemplateParamsMap(Map.of(
            "aaa", 1,
            "bbb", "same"
        ));
        run2.setTemplateParamsMap(Map.of(
            "aaa", 2,
            "bbb", "same"
        ));

        testRuns.add(run3);

        assertDisplayName(run1, PREFIX_1 + " aaa: 1, bbb: same");
        assertDisplayName(run2, PREFIX_2 + " aaa: 2, bbb: same");
        assertDisplayName(run3, PREFIX_3);

        run1.setTemplateParamsMap(Map.of(
            "aaa", "AAA",
            "bbb", "same"
        ));
        run2.setTemplateParamsMap(Map.of(
            "aaa", "AAA",
            "bbb", "same"
        ));

        assertDisplayName(run1, PREFIX_1 + " aaa: AAA, bbb: same");
        assertDisplayName(run2, PREFIX_2 + " aaa: AAA, bbb: same");
        assertDisplayName(run3, PREFIX_3);

        run3.setTestName("otherTest");

        // when the 3rd run is of another test, runs 1+2 no longer have differentiating params to display
        assertDisplayName(run1, PREFIX_1);
        assertDisplayName(run2, PREFIX_2);
        assertDisplayName(run3, PREFIX_3.replace(test.getName(), "otherTest"));
    }
}
