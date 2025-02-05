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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.StreamSupport;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.lifecycle.Managed;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.core.CredentialStore;
import com.datastax.fallout.service.core.Session;
import com.datastax.fallout.service.core.TestCompletionNotification;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.ScopedLogger;

public class UserDAO implements Managed
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(TestRunDAO.class);
    private final CassandraDriverManager driverManager;
    private final Optional<FalloutConfiguration.UserCreds> adminUserCreds;
    private Mapper<Session> sessionMapper;
    private Mapper<User> userMapper;
    private UserAccessor userAccessor;
    private final SecurityUtil securityUtil;
    private final UserGroupMapper userGroupMapper;
    private final CredentialStore credentialStore;

    @Accessor
    private interface UserAccessor
    {
        @Query("SELECT name, email, group FROM users")
        ResultSet getAllNamesEmailsAndGroups();

        @Query("SELECT email FROM users")
        ResultSet getAllEmails();

        @Query("SELECT * FROM users")
        Result<User> getAll();

        @Query("INSERT INTO users(email, name, encpass, salt, admin, oauthId) " +
            "VALUES(:email, :name, :encpass, :salt, :admin, :oauthId) IF NOT EXISTS")
        ResultSet createUserIfNotExists(
            String email, String name, ByteBuffer encpass, ByteBuffer salt, boolean admin, UUID oauthId);
    }

    public UserDAO(CassandraDriverManager driverManager, SecurityUtil securityUtil,
        Optional<FalloutConfiguration.UserCreds> adminUserCreds,
        UserGroupMapper userGroupMapper, CredentialStore credentialStore)
    {
        this.driverManager = driverManager;
        this.securityUtil = securityUtil;
        this.adminUserCreds = adminUserCreds;
        this.userGroupMapper = userGroupMapper;
        this.credentialStore = credentialStore;
    }

    public Session getSession(String token)
    {
        return sessionMapper.get(UUID.fromString(token));
    }

    public Session addSession(User user)
    {
        Session session = new Session(user, FalloutService.COOKIE_NAME);
        sessionMapper.save(session, Mapper.Option.ttl(1209600));

        return session;
    }

    private void addOauthSession(User user)
    {
        Session session = new Session(user, FalloutService.OAUTH_REALM);
        user.setOauthId(session.getTokenId());
        sessionMapper.save(session);
        userMapper.save(user);
    }

    public void addResetToken(User user)
    {
        user.setResetToken(UUID.randomUUID());
        userMapper.save(user);
    }

    public void dropSession(String token)
    {
        if (null != token)
        {
            sessionMapper.delete(UUID.fromString(token));
        }
    }

    public User getUser(String userId)
    {
        var user = userMapper.get(userId, Mapper.Option.consistencyLevel(ConsistencyLevel.SERIAL));
        if (null != user)
        {
            // we don't require a credential store key for user if using LocalCredentialStore
            if (credentialStore instanceof CredentialStore.LocalCredentialStore)
                credentialStore.readUsersCredentialSet(user);
            else if (null != user.getCredentialStoreKey())
                credentialStore.readUsersCredentialSet(user);
        }
        return user;
    }

    public List<Map<String, String>> getAllUsers()
    {
        return getAllUsers(false);
    }

    /** Return a list of users as maps of <code>{"name": ..., "email": ...}</code>, sorted by name.
     * Optionally includes the user's "group".
     * */
    public List<Map<String, String>> getAllUsers(boolean includeGroup)
    {
        return userAccessor.getAllNamesEmailsAndGroups().all().stream()
            .map(r -> {
                var user = ImmutableMap.<String, String>builder()
                    .put("name", r.getString("name"))
                    .put("email", r.getString("email"));
                if (includeGroup)
                {
                    user.put("group", r.getString("group"));
                }
                return (Map<String, String>) user.build();
            })
            .sorted(Comparator.comparing(userMap -> userMap.get("name")))
            .toList();
    }

    public List<String> getAllEmails()
    {
        return userAccessor.getAllEmails().all().stream()
            .map(r -> r.getString("email")).toList();
    }

    /** Adds a new user to the database if they don't already exist; if the add was successful,
     *  then also create a new OAuth session */
    private void createUserIfNotExists(User user)
    {
        try
        {
            credentialStore.createNewUserCredentialSet(user);
        }
        catch (Exception e)
        {
            logger.error("Exception while creating user credential store", e);
            userMapper.delete(user);
            // throw exception to propagate to UI
            throw new RuntimeException("Could not create user credential store");
        }
        var create = userAccessor.createUserIfNotExists(user.getEmail(), user.getName(),
            user.getEncryptedPassword(), user.getSalt(), user.isAdmin(), user.getOauthId());

        if (create.wasApplied())
        {
            addOauthSession(user);
        }
    }

    public User createUserIfNotExists(String name, String email, String password, String group)
    {
        return logger.withScopedDebug("Creating new user").get(() -> {
            User user = makeUser(name, email, password, group);
            logger.withScopedInfo("addUser").run(() -> createUserIfNotExists(user));

            return user;
        });
    }

    private User makeUser(String name, String email, String password, String group)
    {
        User user = new User();

        user.setEmail(email);
        user.setName(name);
        user.setGroup(group);
        user.setSalt(securityUtil.generateSalt());
        user.setEncryptedPassword(securityUtil.getEncryptedPassword(password, user.getSalt()));
        return user;
    }

    public void updateUserCredentials(User user)
    {
        credentialStore.updateUserCredentialsSet(user);
        userMapper.save(user);
    }

    public void updateUserOauthId(User user)
    {
        BatchStatement batchStatement = new BatchStatement();
        batchStatement.add(sessionMapper.deleteQuery(user.getOauthId()));

        user.setOauthId(UUID.randomUUID());

        batchStatement.add(sessionMapper.saveQuery(new Session(user.getOauthId(), user.getEmail(),
            FalloutService.OAUTH_REALM)));
        batchStatement.add(userMapper.saveQuery(user));

        driverManager.getSession().execute(batchStatement);
    }

    public Optional<User> getCIUserByUser(User user)
    {
        Preconditions.checkNotNull(user);
        if (userGroupMapper.isCIUser(user))
        {
            return Optional.of(user);
        }
        return userGroupMapper.findGroup(user.getGroup())
            .flatMap(userGroup -> Optional.ofNullable(getUser(userGroup.userGroupEmail())))
            .map(ciUser -> {
                Preconditions.checkState(userGroupMapper.isCIUser(ciUser));
                return ciUser;
            });
    }

    private void useGeneratedAutomatonSharedHandlesWherePossible()
    {
        logger.withScopedInfo("Looking for explicit User.automatonSharedHandles that could be default-generated")
            .run(() -> StreamSupport.stream(userAccessor.getAll().spliterator(), false)
                .filter(
                    user -> (user.getAutomatonSharedHandle() != null && user.getAutomatonSharedHandle().isEmpty()) ||
                        Objects.equals(user.getAutomatonSharedHandle(), user.getOrGenerateAutomatonSharedHandle()))
                .forEach(user -> {
                    logger.info("  {}: '{}'",
                        user.getEmail(), user.getAutomatonSharedHandle());
                    user.throwOrSetValidAutomatonSharedHandle(null);
                    userMapper.save(user);
                }));
    }

    private void maybeCreateAdminUser()
    {
        adminUserCreds.ifPresent(userCreds -> {
            var user = makeUser(userCreds.name(), userCreds.email(), userCreds.password(),
                UserGroupMapper.UserGroup.OTHER);
            user.setAdmin(true);
            createUserIfNotExists(user);
        });
    }

    @Override
    public void start() throws Exception
    {
        // Since cassandra-driver-mapping 3.0, enum deserializers have to be explicitly registered
        CodecRegistry.DEFAULT_INSTANCE.register(new EnumNameCodec<TestCompletionNotification>(
            TestCompletionNotification.class));

        sessionMapper = driverManager.getMappingManager().mapper(Session.class);
        userMapper = driverManager.getMappingManager().mapper(User.class);
        userAccessor = driverManager.getMappingManager().createAccessor(UserAccessor.class);

        if (driverManager.isSchemaCreator())
        {
            useGeneratedAutomatonSharedHandlesWherePossible();
            maybeCreateAdminUser();
        }
    }

    @Override
    public void stop() throws Exception
    {

    }
}
