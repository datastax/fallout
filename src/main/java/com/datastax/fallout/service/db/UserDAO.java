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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

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

    @Accessor
    private interface UserAccessor
    {
        @Query("SELECT name, email FROM users")
        ResultSet getAllNamesAndEmails();

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
        Optional<FalloutConfiguration.UserCreds> adminUserCreds)
    {
        this.driverManager = driverManager;
        this.securityUtil = securityUtil;
        this.adminUserCreds = adminUserCreds;
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
        return userMapper.get(userId, Mapper.Option.consistencyLevel(ConsistencyLevel.SERIAL));
    }

    public List<Map<String, String>> getAllUsers()
    {
        return userAccessor.getAllNamesAndEmails().all().stream()
            .map(r -> ImmutableMap.of("name", r.getString("name"), "email", r.getString("email")))
            .collect(Collectors.toList());
    }

    public List<String> getAllEmails()
    {
        return userAccessor.getAllEmails().all().stream()
            .map(r -> r.getString("email")).collect(Collectors.toList());
    }

    /** Adds a new user to the database if they don't already exist; if the add was successful,
     *  then also create a new OAuth session */
    private void createUserIfNotExists(User user)
    {
        var create = userAccessor.createUserIfNotExists(user.getEmail(), user.getName(),
            user.getEncryptedPassword(), user.getSalt(), user.isAdmin(), user.getOauthId());

        if (create.wasApplied())
        {
            addOauthSession(user);
        }
    }

    public User createUserIfNotExists(String name, String email, String password)
    {
        try (ScopedLogger.Scoped ignored = logger.scopedDebug("Creating new user"))
        {
            User user = makeUser(name, email, password);
            logger.doWithScopedInfo(() -> createUserIfNotExists(user), "addUser");

            return user;
        }
    }

    private User makeUser(String name, String email, String password)
    {
        User user = new User();

        user.setEmail(email);
        user.setName(name);

        user.setSalt(securityUtil.generateSalt());
        user.setEncryptedPassword(securityUtil.getEncryptedPassword(password, user.getSalt()));
        return user;
    }

    public void updateUserCredentials(User user)
    {
        userMapper.save(user);
    }

    public void updateUserCredentials(User user, Mapper.Option option)
    {
        userMapper.save(user, option);
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

    private void maybeCreateAdminUser()
    {
        adminUserCreds.ifPresent(userCreds -> {
            var user = makeUser(userCreds.getName(), userCreds.getEmail(), userCreds.getPassword());
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
            maybeCreateAdminUser();
        }
    }

    @Override
    public void stop() throws Exception
    {

    }
}
