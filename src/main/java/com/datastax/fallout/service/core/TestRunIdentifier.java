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
package com.datastax.fallout.service.core;

import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;

import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.UDT;

/** Also used as part of {@link com.datastax.fallout.runner.TestRunStatusUpdate} to communicate between
 *  processes; check the documentation for that class for what to bear in mind when making changes */
@UDT(name = "testRunIdentifier")
public class TestRunIdentifier
{
    @Field
    private String testOwner;

    @Field
    private String testName;

    @Field
    private UUID testRunId;

    /** Needed by the C* Mapper */
    @SuppressWarnings("unused")
    private TestRunIdentifier()
    {
    }

    @JsonCreator
    public TestRunIdentifier(String testOwner, String testName, UUID testRunId)
    {
        this.testOwner = testOwner;
        this.testName = testName;
        this.testRunId = testRunId;
    }

    public String getTestOwner()
    {
        return testOwner;
    }

    public String getTestName()
    {
        return testName;
    }

    public UUID getTestRunId()
    {
        return testRunId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof TestRunIdentifier))
        {
            return false;
        }
        TestRunIdentifier that = (TestRunIdentifier) o;
        return testOwner.equals(that.testOwner) &&
            testName.equals(that.testName) &&
            testRunId.equals(that.testRunId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(testOwner, testName, testRunId);
    }

    @Override
    public String toString()
    {
        return "TestRunIdentifier{" +
            "testOwner='" + testOwner + '\'' +
            ", testName='" + testName + '\'' +
            ", testRunId=" + testRunId +
            '}';
    }
}
