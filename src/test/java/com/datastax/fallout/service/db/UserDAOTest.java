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
package com.datastax.fallout.service.db;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.service.FalloutConfiguration.UserCreds;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.core.CredentialStore;
import com.datastax.fallout.test.utils.WithTestResources;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;

@Tag("requires-db")
public class UserDAOTest extends WithTestResources
{
    private static final String keyspace = "user_dao";
    private CassandraDriverManager driverManager;
    private UserDAO userDAO;
    private SecurityUtil securityUtil;

    public void startUserDAO(Optional<UserCreds> adminUserCreds) throws Exception
    {
        driverManager = createDriverManager(keyspace);

        securityUtil = new SecurityUtil();
        userDAO = new UserDAO(driverManager, securityUtil, adminUserCreds, UserGroupMapper.empty(),
            new CredentialStore.NoopCredentialStore());

        driverManager.start();
        userDAO.start();
    }

    @AfterEach
    public void stopUserDAO() throws Exception
    {
        userDAO.stop();
        driverManager.stop();
    }

    @Test
    public void default_admin_user_is_created_when_usercreds_specified() throws Exception
    {
        final var adminUserCreds =
            UserCreds.from("admin:admin@example.com:horse-battery-staple");

        startUserDAO(Optional.of(adminUserCreds));

        assertThat(userDAO.getAllUsers()).hasSize(1);

        final var adminUser = userDAO.getUser(adminUserCreds.email());
        assertThat(adminUser)
            .isAdmin()
            .hasEmail(adminUserCreds.email())
            .hasName(adminUserCreds.name());

        assertThat(securityUtil.authenticate(adminUserCreds.password(),
            adminUser.getEncryptedPassword(), adminUser.getSalt())).isTrue();

        assertThat(userDAO.getSession(adminUser.getOauthId().toString()).getUserId())
            .isEqualTo(adminUserCreds.email());
    }

    @Test
    public void default_admin_user_is_not_created_when_usercreds_not_specified() throws Exception
    {
        startUserDAO(Optional.empty());

        assertThat(userDAO.getAllUsers()).isEmpty();
    }
}
