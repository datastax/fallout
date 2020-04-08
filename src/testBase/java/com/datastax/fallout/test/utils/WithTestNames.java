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
package com.datastax.fallout.test.utils;

import java.util.Objects;
import java.util.Optional;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runner.Description;

public abstract class WithTestNames
{
    @ClassRule
    public static final TestClassRule testDescription = new TestClassRule();

    @Rule
    public final TestName testName = new TestName();

    protected static Class<?> currentTestClass()
    {
        return testDescription.getTestClass();
    }

    public static String currentTestClassName(Description description)
    {
        return TestClassRule.getTestClassName(description.getTestClass());
    }

    protected static String currentTestClassName()
    {
        return testDescription.getTestClassName();
    }

    private static String fileNameFromTestRunName(String testRunName)
    {
        int startIdx = testRunName.indexOf("[");
        int endIdx = testRunName.indexOf(".yaml]");

        // Replace testRunNames with a YAML file as the parameter with just the YAML file
        if (startIdx > 0 && endIdx > startIdx)
        {
            // testExampleValidation[ExampleYaml-my-test-name.yaml]
            // -> ExampleYaml-my-test-name
            testRunName = testRunName.substring(startIdx + 1, endIdx);
            // ExampleYaml-kubernetes/gke-bench
            // -> ExampleYaml-kubernetes-gke-bench
            testRunName = testRunName.replaceAll("/", "-");
        }

        // Replace the parameter of testRunNames with a hash code (to prevent filename length errors).
        else if (startIdx > 0 && testRunName.charAt(testRunName.length() - 1) == ']')
        {
            endIdx = testRunName.indexOf("]");
            if (endIdx > startIdx)
            {
                final int hashCode = Objects.hashCode(testRunName);
                testRunName =
                    testRunName.substring(0, startIdx + 1) +
                        Integer.toHexString(hashCode) + "]";
            }
        }
        return testRunName;
    }

    public static Optional<String> currentTestShortName(Description description)
    {
        return Optional.ofNullable(description.getMethodName())
            .map(WithTestNames::fileNameFromTestRunName);
    }

    protected String currentTestShortName()
    {
        return fileNameFromTestRunName(testName.getMethodName());
    }
}
