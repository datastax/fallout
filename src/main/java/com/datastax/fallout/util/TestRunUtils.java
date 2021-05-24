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
package com.datastax.fallout.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.fallout.service.core.TestRun;

import static java.util.stream.Collectors.toList;

public class TestRunUtils
{
    private TestRunUtils()
    {
        // utility class
    }

    private static final Set<String> PARAMS_TO_ALWAYS_DISPLAY = Set.of("dse_version", "product_version",
        "note", "notes", "comment");

    private static String paramValueString(Object paramValue)
    {
        // FAL-1497: get rid of newlines that might accidentally be in the param value.
        // the test runs most likely behaved in the same way since when we turn some values
        // into shell cmds the extra spaces dont matter
        return String.valueOf(paramValue).trim();
    }

    private static Map<TestRun, String> buildUniqueParamInfo(List<TestRun> testRuns)
    {
        Map<String, List<String>> allParams = new HashMap<>();
        for (TestRun run : testRuns)
        {
            for (Map.Entry<String, Object> e : run.getTemplateParamsMap().entrySet())
            {
                String param = e.getKey();
                // since in the end we output as string, we compare values by their string representation
                String paramValue = paramValueString(e.getValue());
                List<String> seenParamValues = allParams.computeIfAbsent(param, s -> new ArrayList<>());
                seenParamValues.add(paramValue);
            }
        }
        Set<String> differingParams = new HashSet<>();
        for (Map.Entry<String, List<String>> e : allParams.entrySet())
        {
            String param = e.getKey();
            List<String> seenParamValues = e.getValue();
            Set<String> uniqueParamValues = new HashSet<>();
            for (String seenParamValue : seenParamValues)
            {
                uniqueParamValues.add(seenParamValue);
            }
            boolean notAllRunsHaveThisParam = seenParamValues.size() != testRuns.size();
            boolean notAllRunsHaveSameValueForThisParam = uniqueParamValues.size() != 1;
            if (notAllRunsHaveThisParam || notAllRunsHaveSameValueForThisParam)
            {
                differingParams.add(param);
            }
        }

        Map<TestRun, String> testRunFullParamStrings = new HashMap<>();
        for (TestRun run : testRuns)
        {
            List<String> paramsToDisplay = new ArrayList<>();

            List<String> testRunParams = new ArrayList<>(run.getTemplateParamsMap().keySet());
            testRunParams.sort(String::compareTo);
            for (String param : testRunParams)
            {
                boolean shouldDisplayParam = differingParams.contains(param) || PARAMS_TO_ALWAYS_DISPLAY
                    .contains(param);
                if (shouldDisplayParam)
                {
                    String paramValue = paramValueString(run.getTemplateParamsMap().get(param));
                    paramsToDisplay.add(param + ": " + paramValue);
                }
            }
            String fullParamString = String.join(", ", paramsToDisplay);
            testRunFullParamStrings.put(run, fullParamString);
        }
        return testRunFullParamStrings;
    }

    public static Map<TestRun, String> buildTestRunDisplayNames(List<TestRun> testRuns)
    {
        return buildTestRunDisplayNames(testRuns, true);
    }

    public static Map<TestRun, String> buildTestRunDisplayNames(List<TestRun> testRuns, boolean testNamePrefix)
    {
        Map<TestRun, String> defaultDisplayNames = new HashMap<>();
        Set<String> seenTestNames = new HashSet<>();
        for (TestRun run : testRuns)
        {
            seenTestNames.add(run.getTestName());
            String displayName = run.buildShortTestRunId() + " " + DateUtils.formatUTCDate(run.getStartedAt());
            if (testNamePrefix)
            {
                displayName = run.getTestName() + " " + displayName;
            }
            defaultDisplayNames.put(run, displayName);
        }
        Map<TestRun, String> parameterAwareDisplayNames = new HashMap<>();
        for (String testName : seenTestNames)
        {
            List<TestRun> runsForTest = testRuns.stream()
                .filter(r -> r.getTestName().equals(testName))
                .collect(toList());
            Map<TestRun, String> uniqueParamInfo = buildUniqueParamInfo(runsForTest);
            for (TestRun run : runsForTest)
            {
                String displayName = defaultDisplayNames.get(run);
                String uniqueParamString = uniqueParamInfo.get(run);
                if (!uniqueParamString.isEmpty())
                {
                    displayName += " " + uniqueParamString;
                }
                parameterAwareDisplayNames.put(run, displayName.strip());
            }
        }
        return parameterAwareDisplayNames;
    }

}
