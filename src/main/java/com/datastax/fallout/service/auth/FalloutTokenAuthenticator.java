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
package com.datastax.fallout.service.auth;

import java.util.Optional;

import io.dropwizard.auth.AuthenticationException;
import io.dropwizard.auth.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.core.Session;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.db.UserDAO;

public class FalloutTokenAuthenticator implements Authenticator<String, User>
{
    private static final Logger logger = LoggerFactory.getLogger(FalloutTokenAuthenticator.class);
    private final UserDAO userDao;
    private final String tokenType;

    public FalloutTokenAuthenticator(UserDAO userDao, String tokenType)
    {
        this.userDao = userDao;
        this.tokenType = tokenType;
    }

    @Override
    public Optional<User> authenticate(String token) throws AuthenticationException
    {
        try
        {
            Session session = userDao.getSession(token);
            if (session != null && !session.getTokenType().equals(tokenType))
            {
                logger.error("Used " + session.getTokenType() + " type token for " + tokenType + " authenticator.");
                return Optional.empty();
            }

            if (session != null && session.getUserId() != null)
            {
                String userId = session.getUserId();
                User user = userDao.getUser(userId);
                logger.info("Logged in user: " + userId + " (" + user + ")");
                return Optional.of(user);
            }

            logger.info("Failed to authenticate token: " + token);
            return Optional.empty();
        }
        catch (Exception e)
        {
            throw new AuthenticationException(e);
        }
    }
}
