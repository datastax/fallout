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
package com.datastax.fallout.service.db;

import java.util.HashSet;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.datastax.driver.core.Session;
import com.datastax.fallout.service.FalloutConfiguration.UserCreds;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.core.Fakes;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.core.User.GoogleCloudServiceAccount;
import com.datastax.fallout.test.utils.WithTestResources;
import com.datastax.fallout.test.utils.categories.RequiresDb;

import static com.datastax.fallout.service.core.UserAssert.assertThat;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;
import static org.assertj.core.api.Assertions.assertThat;

@Category(RequiresDb.class)
public class UserDAOTest extends WithTestResources
{
    private static final String keyspace = "user_dao";
    private CassandraDriverManager driverManager;
    private UserDAO userDAO;
    private Session session;
    private SecurityUtil securityUtil;

    private User user;

    public void startUserDAO(Optional<UserCreds> adminUserCreds) throws Exception
    {
        driverManager = createDriverManager(keyspace);

        securityUtil = new SecurityUtil();
        userDAO = new UserDAO(driverManager, securityUtil, adminUserCreds);

        driverManager.start();
        userDAO.start();

        session = driverManager.getSession();

        user = Fakes.makeUser();
        user.setGoogleCloudServiceAccounts(new HashSet<>());
    }

    @After
    public void stopUserDAO() throws Exception
    {
        userDAO.stop();
        driverManager.stop();
    }

    private void assertThatPersistingSatisfies(Runnable assertions)
    {
        assertions.run();
        userDAO.updateUserCredentials(user);
        user = userDAO.getUser(user.getEmail());
        assertions.run();
    }

    @Test
    public void default_admin_user_is_created_when_usercreds_specified() throws Exception
    {
        final var adminUserCreds =
            UserCreds.from("admin:admin@example.com:horse-battery-staple");

        startUserDAO(Optional.of(adminUserCreds));

        assertThat(userDAO.getAllUsers()).hasSize(1);

        final var adminUser = userDAO.getUser(adminUserCreds.getEmail());
        assertThat(adminUser)
            .isAdmin()
            .hasEmail(adminUserCreds.getEmail())
            .hasName(adminUserCreds.getName());

        assertThat(securityUtil.authenticate(adminUserCreds.getPassword(),
            adminUser.getEncryptedPassword(), adminUser.getSalt())).isTrue();

        assertThat(userDAO.getSession(adminUser.getOauthId().toString()).getUserId())
            .isEqualTo(adminUserCreds.getEmail());
    }

    @Test
    public void default_admin_user_is_not_created_when_usercreds_not_specified() throws Exception
    {
        startUserDAO(Optional.empty());

        assertThat(userDAO.getAllUsers()).isEmpty();
    }

    @Test
    public void google_cloud_service_accounts_are_persisted_correctly() throws Exception
    {
        startUserDAO(Optional.empty());

        final String email1 = "service-account-user@project-name.iam.gserviceaccount.com";
        final String privateKeyId1 = "randomstringofcharactersexceptitsanumber";

        final String serviceAccount1Json = getTestClassResource(
            "fake-service-account.json");

        final GoogleCloudServiceAccount serviceAccount1 = GoogleCloudServiceAccount.fromJson(serviceAccount1Json);

        user.addGoogleCloudServiceAccount(serviceAccount1);

        assertThatPersistingSatisfies(() -> {
            assertThat(user).hasDefaultGoogleCloudServiceAccountEmail(email1);
            assertThat(user).hasOnlyGoogleCloudServiceAccounts(serviceAccount1);
            assertThat(user.getGoogleCloudServiceAccounts()).hasOnlyOneElementSatisfying(serviceAccount -> {
                assertThat(serviceAccount.email).isEqualTo(email1);
                assertThat(serviceAccount.project).isEqualTo("project-name");
                assertThat(serviceAccount.privateKeyId).isEqualTo(privateKeyId1);
            });
        });

        final String email2 = "service-account-user2@project-name.iam.gserviceaccount.com";
        final String privateKeyId2 = "5c30197f75c1ebf1e472ee113e5177a85bb1305b";
        final String serviceAccount2Json = serviceAccount1Json
            .replace(email1, email2)
            .replace(privateKeyId1, privateKeyId2);

        final GoogleCloudServiceAccount serviceAccount2 = GoogleCloudServiceAccount.fromJson(serviceAccount2Json);

        user.addGoogleCloudServiceAccount(serviceAccount2);

        assertThatPersistingSatisfies(() -> {
            assertThat(user).hasDefaultGoogleCloudServiceAccountEmail(email1);
            assertThat(user).hasOnlyGoogleCloudServiceAccounts(serviceAccount1, serviceAccount2);
        });

        user.validateAndSetDefaultGoogleCloudServiceAccount(email2);

        assertThatPersistingSatisfies(() -> {
            assertThat(user).hasDefaultGoogleCloudServiceAccountEmail(email2);
            assertThat(user).hasOnlyGoogleCloudServiceAccounts(serviceAccount1, serviceAccount2);
        });

        user.dropGoogleCloudServiceAccount(email2);

        assertThatPersistingSatisfies(() -> {
            assertThat(user).hasDefaultGoogleCloudServiceAccountEmail(email1);
            assertThat(user).hasOnlyGoogleCloudServiceAccounts(serviceAccount1);
        });
    }
}
