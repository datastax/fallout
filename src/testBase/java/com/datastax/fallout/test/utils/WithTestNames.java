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
package com.datastax.fallout.test.utils;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public abstract class WithTestNames
{
    private static Class<?> currentTestClass;
    private Method currentTestMethod;
    private String currentTestDisplayName;

    private static final Pattern YAML_FILENAME_MATCHER = Pattern.compile("\\[([^]]*\\.yaml)\\]");
    private static final int MAX_SUFFIX_LEN = 30;

    @BeforeAll
    static void beforeAll(TestInfo testInfo)
    {
        assertThat(testInfo.getTestClass()).isNotEmpty();
        currentTestClass = testInfo.getTestClass().get();
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo)
    {
        assertThat(testInfo.getTestMethod()).isNotEmpty();
        currentTestMethod = testInfo.getTestMethod().get();
        currentTestDisplayName = testInfo.getDisplayName();
    }

    protected static Class<?> currentTestClass()
    {
        return currentTestClass;
    }

    protected static String testClassName(Class<?> testClass)
    {
        return testClass.getSimpleName();
    }

    protected static String currentTestClassName()
    {
        return testClassName(currentTestClass());
    }

    private static String stripBrackets(String displayName)
    {
        final var trimmedDisplayName = displayName.trim();
        return trimmedDisplayName.startsWith("[") && trimmedDisplayName.endsWith("]") ?
            trimmedDisplayName.substring(1, trimmedDisplayName.length() - 1) :
            trimmedDisplayName;
    }

    protected static String fileNameFromTestRunName(Method testMethod, String displayName)
    {
        final var methodName = testMethod.getName();
        // The default displayName in JUnit 5 is the method name + "()"
        final var suffix = !displayName.equals(methodName + "()") ?
            Optional.of(stripBrackets(displayName)) : Optional.<String>empty();

        // Replace suffixes that specify a YAML file as the parameter with just the YAML file e.g.
        // testExampleValidation[ExampleYaml-foo-2/some-test.yaml]
        // -> ExampleYaml-foo-2-some-test.yaml
        final var exampleYamlFileName =
            suffix.filter(suffix_ -> suffix_.endsWith(".yaml"))
                .map(suffix_ -> suffix_.replaceAll("/", "-"));

        final var truncatedSuffix =
            suffix
                .map(suffix_ -> suffix_.length() > MAX_SUFFIX_LEN ?
                    suffix_.substring(0, MAX_SUFFIX_LEN - 11) + "..." + Integer.toHexString(Objects.hashCode(suffix_)) :
                    suffix_)
                .map(suffix_ -> "[" + suffix_.replaceAll(" ", "-") + "]");

        return exampleYamlFileName
            .orElse(methodName + truncatedSuffix.orElse(""));
    }

    protected String currentTestShortName()
    {
        return fileNameFromTestRunName(currentTestMethod, currentTestDisplayName);
    }
}
