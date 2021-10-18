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

import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FilenameUtils;

import com.datastax.fallout.service.db.UserGroupMapper;
import com.datastax.fallout.test.utils.WithTestResources;
import com.datastax.fallout.util.FileUtils;

public class Fakes
{
    private static final String TEST_USER_NAME = "fallout-unittest-user";
    public static final String TEST_USER_EMAIL = TEST_USER_NAME + "@example.com";
    public static final String TEST_NAME = "testName";
    public static final UUID TEST_RUN_ID = UUID.fromString("FA110072-D811-4B2C-B08F-000000000000");
    public static final TestRunIdentifier TEST_RUN_IDENTIFIER =
        new TestRunIdentifier(TEST_USER_EMAIL, TEST_NAME, TEST_RUN_ID);
    private static final String UNIT_TEST_GOOGLE_SERVICE_ACCOUNT_JSON_FILE =
        "UNIT_TEST_GOOGLE_SERVICE_ACCOUNT_JSON_FILE";
    public static final String TEST_REGISTRY = "docker-registry.example.com";

    public static User makeUser()
    {
        User user = new User();
        user.setName(TEST_USER_NAME);
        user.setEmail(TEST_USER_EMAIL);
        user.setGroup(UserGroupMapper.UserGroup.OTHER);
        user.addNebulaAppCred(new User.NebulaAppCred("bogus", "bogus", "bogus", "bogus", "bogus"));
        // the following creds are needed for examples + ATRB-Test
        user.addNebulaAppCred(
            new User.NebulaAppCred("moonshot-v1", "moon_v1", "fake-secret", "fake-access", "fake-secret"));
        user.addNebulaAppCred(
            new User.NebulaAppCred("moonshot-v2", "moon_v2", "fake-secret", "fake-access", "fake-secret"));
        user.addNebulaAppCred(
            new User.NebulaAppCred("dse-automation", "dse", "fake-secret", "fake-access", "fake-secret"));
        user.addGoogleCloudServiceAccount(createGCloudServiceAccount());
        user.addAstraCred(new User.AstraServiceAccount(
            "fake-client-id",
            "fake-client-name",
            "fake-client-secret"));
        user.addDockerRegistryCredential(new User.DockerRegistryCredential(TEST_REGISTRY, "user", "docker-password"));
        return user;
    }

    private static User.GoogleCloudServiceAccount createGCloudServiceAccount()
    {
        String existingKeyFileJsonFromEnv = System.getenv(UNIT_TEST_GOOGLE_SERVICE_ACCOUNT_JSON_FILE);
        if (existingKeyFileJsonFromEnv != null)
        {
            return User.GoogleCloudServiceAccount.fromJson(
                FileUtils.readString(Paths.get(existingKeyFileJsonFromEnv)));
        }
        return new User.GoogleCloudServiceAccount() {
            {
                email = "fake-service-account@google-cloud.example.com";
                project = "fake-project";
                privateKeyId = "fake-private-key-id";
                keyFileJson = "{\"fake\": 1}";
            }
        };
    }

    public static class UUIDFactory
    {
        private UUID currentUuid = TEST_RUN_ID;

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
            return makeTestRun(test, Map.of());
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
