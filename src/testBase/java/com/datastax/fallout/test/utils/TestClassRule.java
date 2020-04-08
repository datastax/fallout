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

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestClassRule extends TestWatcher
{
    private Class<?> testClass;

    @Override
    protected void starting(Description description)
    {
        this.testClass = description.getTestClass();
    }

    protected Class<?> getTestClass()
    {
        return testClass;
    }

    public static String getTestClassName(Class<?> testClass)
    {
        return testClass.getSimpleName();
    }

    protected String getTestClassName()
    {
        return getTestClassName(testClass);
    }
}
