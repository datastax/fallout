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
package com.datastax.fallout.runner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;

import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;

/** Used to communicate the current state of a {@link TestRun} via {@link
 * com.datastax.fallout.service.resources.runner.RunnerResource#statusFeed}.  When making
 * changes to this class be aware that the QUEUE process must be able to handle older versions
 * of this class being sent by RUNNER processes running an earlier version of fallout */
@AutoValue
@AutoValue.CopyAnnotations
@JsonSerialize(as = TestRunStatusUpdate.class)
public abstract class TestRunStatusUpdate
{
    public abstract TestRunIdentifier getTestRunIdentifier();

    public abstract TestRun.State getState();

    @JsonCreator
    static public TestRunStatusUpdate of(TestRunIdentifier testRunIdentifier, TestRun.State state)
    {
        return new AutoValue_TestRunStatusUpdate(testRunIdentifier, state);
    }
}
