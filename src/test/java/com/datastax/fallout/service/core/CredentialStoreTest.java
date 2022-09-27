/*
 * Copyright 2022 DataStax, Inc.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.service.db.UserGroupMapper;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;
import static com.datastax.fallout.service.db.CassandraDriverManagerHelpers.createDriverManager;

@Tag("requires-db")
public class CredentialStoreTest
{
    private static class InMemoryCredentialStore extends CredentialStore
    {
        private final Map<String, User.CredentialSet> store = new HashMap<>();

        @Override
        public User.CredentialSet getCredentialsFromStore(User user)
        {
            Preconditions.checkState(store.containsKey(user.getCredentialStoreKey()));
            return store.get(user.getCredentialStoreKey());
        }

        @Override
        public void updateCredentialsInStore(User user)
        {
            if (store.containsKey(user.getCredentialStoreKey()))
            {
                store.put(user.getCredentialStoreKey(), user.getCredentialsSet());
            }
        }

        @Override
        public void createNewCredentialsInStore(User user)
        {
            Preconditions.checkState(!store.containsKey(user.getCredentialStoreKey()));
            var key = user.getEmail();
            store.put(key, user.getCredentialsSet());
            user.setCredentialStoreKey(key);
        }
    }

    private static UserDAO userDAO;
    private static InMemoryCredentialStore credStore;

    @BeforeAll
    public static void startCassandra() throws Exception
    {
        CassandraDriverManager driverManager = createDriverManager("cred_store_test");
        credStore = new InMemoryCredentialStore();
        userDAO = new UserDAO(driverManager, new SecurityUtil(), Optional.empty(), UserGroupMapper.empty(),
            credStore);
        driverManager.start();
        userDAO.start();
    }

    @Test
    public void user_credentials_are_fetched_transparently_and_in_sync_from_store_when_accessing_through_user_dao()
    {
        assertThat(credStore.store).isEmpty();

        userDAO.createUserIfNotExists("owner", TEST_USER_EMAIL, "", UserGroupMapper.UserGroup.OTHER);
        assertThat(credStore.store).containsKey(TEST_USER_EMAIL);

        User user = userDAO.getUser(TEST_USER_EMAIL);
        assertThat(user.getCredentialStoreKey()).isEqualTo(TEST_USER_EMAIL);
        assertThat(user.getGenericSecrets()).isEmpty();
        user.addGenericSecret("a", "secret");
        user.addGenericSecret("b", "even more secret");

        userDAO.updateUserCredentials(user);
        assertThat(credStore.store).containsKey(TEST_USER_EMAIL);

        User userObjB = userDAO.getUser(TEST_USER_EMAIL);
        assertThat(userObjB.getGenericSecrets()).isEqualTo(
            Map.of("a", "secret", "b", "even more secret"));

        userObjB.dropGenericSecret("a");
        userDAO.updateUserCredentials(userObjB);

        User userObjC = userDAO.getUser(TEST_USER_EMAIL);
        assertThat(userObjC.getGenericSecrets()).isEqualTo(Map.of("b", "even more secret"));
    }
}
