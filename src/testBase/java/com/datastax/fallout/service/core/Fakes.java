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
package com.datastax.fallout.service.core;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import com.datastax.fallout.test.utils.WithTestResources;
import com.datastax.fallout.util.Exceptions;

public class Fakes
{
    private static final String TEST_USER_NAME = "fallout-unittest-user";
    public static final String TEST_USER_EMAIL = TEST_USER_NAME + "@example.com";
    public static final String TEST_NAME = "testName";
    private static final String UNIT_TEST_GOOGLE_SERVICE_ACCOUNT_JSON_FILE =
        "UNIT_TEST_GOOGLE_SERVICE_ACCOUNT_JSON_FILE";

    public static User makeUser()
    {
        User user = new User();
        user.setName(TEST_USER_NAME);
        user.setEmail(TEST_USER_EMAIL);
        user.addGoogleCloudServiceAccount(new User.GoogleCloudServiceAccount()
        {
            {
                email = "fake-service-account@google-cloud.example.com";
                project = "fake-project";
                privateKeyId = "fake-private-key-id";
                keyFileJson = maybeGetKeyFileJsonFromEnv();
            }
        });
        return user;
    }

    private static String maybeGetKeyFileJsonFromEnv()
    {
        String existingKeyFileJsonFromEnv = System.getenv(UNIT_TEST_GOOGLE_SERVICE_ACCOUNT_JSON_FILE);
        if (existingKeyFileJsonFromEnv == null)
        {
            return "{\"fake\": 1}";
        }
        return Exceptions.getUncheckedIO(
            () -> FileUtils.readFileToString(Paths.get(existingKeyFileJsonFromEnv).toFile(), StandardCharsets.UTF_8));
    }

    public static class UUIDFactory
    {
        private UUID currentUuid = UUID.fromString("FA110072-D811-4B2C-B08F-000000000000");

        public UUIDFactory()
        {
        }

        public UUIDFactory(UUID initialUuid)
        {
            currentUuid = initialUuid;
        }

        public UUID create()
        {
            currentUuid = new UUID(currentUuid.getMostSignificantBits(),
                currentUuid.getLeastSignificantBits() + 1);
            return currentUuid;
        }
    }

    public static class TestRunFactory
    {
        private UUIDFactory uuidFactory = new UUIDFactory();

        public TestRunFactory()
        {
        }

        public TestRunFactory(UUID initialTestRunUuid)
        {
            uuidFactory = new UUIDFactory(initialTestRunUuid);
        }

        public TestRun makeTestRun(Test test)
        {
            return makeTestRun(test, Collections.emptyMap());
        }

        public TestRun makeTestRun(Test test, Map<String, Object> templateParams)
        {
            TestRun testRun = test.createTestRun(templateParams);
            testRun.setTestRunId(uuidFactory.create());
            return testRun;
        }
    }

    public static Test makeTest(User user, Class<?> testClass, String resourcePath)
    {
        return Test.createTest(
            user.getEmail(),
            FilenameUtils.getBaseName(resourcePath),
            WithTestResources.getTestClassResource(testClass, resourcePath));
    }
}
