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

import java.nio.file.Path;

import com.datastax.fallout.ops.JobFileLoggers;
import com.datastax.fallout.ops.JobLoggers;
import com.datastax.fallout.service.core.TestRun;

import static com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;

public class JobLoggersFactory
{
    private final Path artifactPathRoot;
    private final boolean logTestRunsToConsole;

    public JobLoggersFactory(Path artifactPathRoot, boolean logTestRunsToConsole)
    {
        this.artifactPathRoot = artifactPathRoot;
        this.logTestRunsToConsole = logTestRunsToConsole;
    }

    JobLoggers create(TestRun testRun, UserCredentials userCredentials)
    {
        return new JobFileLoggers(
            Artifacts.buildTestRunArtifactPath(artifactPathRoot, testRun), logTestRunsToConsole, userCredentials);
    }
}
