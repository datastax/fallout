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
package com.datastax.fallout.service.artifacts;

import java.nio.file.Path;
import java.util.UUID;

import com.datastax.fallout.runner.Artifacts;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;

import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;

class ManagedArtifactServiceTest extends WithPersistentTestOutputDir
{
    TestRun createTestRun(String testName, String testRunId)
    {
        TestRun testRun = new TestRun();
        testRun.setOwner(TEST_USER_EMAIL);
        testRun.setTestName(testName);
        testRun.setTestRunId(UUID.fromString(testRunId));

        return testRun;
    }

    Path artifactRootPath()
    {
        return persistentTestOutputDir();
    }

    Path artifactPathForTestRun(String testName, String testRunId)
    {
        Path artifactPath;
        TestRun testRun = createTestRun(testName, testRunId);
        artifactPath = Artifacts.buildTestRunArtifactPath(
            artifactRootPath(), testRun);

        return artifactPath;
    }
}
