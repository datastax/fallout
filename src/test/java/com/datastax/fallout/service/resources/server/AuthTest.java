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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import java.util.UUID;

import io.dropwizard.setup.Environment;
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.resources.AccountClient;
import com.datastax.fallout.service.resources.FalloutAppExtension;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.FalloutConfiguration.AuthenticationMode.SINGLE_USER;

@Tag("requires-db")
public abstract class AuthTest
{
    static final String userCredsString = "user:email@email.com:password";
    static final FalloutConfiguration.UserCreds userCreds = FalloutConfiguration.UserCreds.from(userCredsString);

    abstract FalloutAppExtension getServiceExtension();

    abstract FalloutAppExtension.FalloutServiceResetExtension getResetExtension();

    Invocation.Builder accountTarget()
    {
        return getResetExtension().anonApi().target("/account").request();
    }

    @Test
    public void test_auth_using_oauth_token()
    {
        UUID oauthToken = AccountClient.getOAuthId(getServiceExtension().getSession(), userCreds.email());
        String header = String.format("%s %s", FalloutService.OAUTH_BEARER_TOKEN_TYPE, oauthToken);
        assertThat(accountTarget().header("Authorization", header).get()).hasStatusInfo(Response.Status.OK);
    }

    public static class SingleUser extends AuthTest
    {
        @RegisterExtension
        public static final FalloutAppExtension FALLOUT_APP_EXTENSION;

        @RegisterExtension
        public final FalloutAppExtension.FalloutServiceResetExtension resetExtension =
            FALLOUT_APP_EXTENSION.resetExtension(FalloutAppExtension.ResetBehavior.DO_NOT_CLEAN_DATABASE);

        static
        {
            FALLOUT_APP_EXTENSION = new FalloutAppExtension();
            FALLOUT_APP_EXTENSION.addListener(new DropwizardAppExtension.ServiceListener<FalloutConfiguration>() {
                @Override
                public void onRun(FalloutConfiguration configuration, Environment environment,
                    DropwizardAppExtension<FalloutConfiguration> rule)
                {
                    configuration.setAdminUserCreds(userCredsString);
                    configuration.setAuthenticationMode(SINGLE_USER);
                }
            });
        }

        @Override
        protected FalloutAppExtension getServiceExtension()
        {
            return FALLOUT_APP_EXTENSION;
        }

        @Override
        FalloutAppExtension.FalloutServiceResetExtension getResetExtension()
        {
            return resetExtension;
        }

        @Test
        public void test_auth_without_cookie()
        {
            assertThat(accountTarget().get()).hasStatusInfo(Response.Status.OK);
        }
    }

    public static class MultiUser extends AuthTest
    {
        @RegisterExtension
        public static final FalloutAppExtension FALLOUT_APP_EXTENSION = new FalloutAppExtension();

        @RegisterExtension
        public final FalloutAppExtension.FalloutServiceResetExtension resetExtension =
            FALLOUT_APP_EXTENSION.resetExtension();

        private static Cookie userCookie;

        @Override
        protected FalloutAppExtension getServiceExtension()
        {
            return FALLOUT_APP_EXTENSION;
        }

        @Override
        FalloutAppExtension.FalloutServiceResetExtension getResetExtension()
        {
            return resetExtension;
        }

        @BeforeEach
        public void registerAccount()
        {
            MultivaluedMap<String, String> registerForm = new MultivaluedHashMap<>();
            registerForm.add("name", userCreds.name());
            registerForm.add("email", userCreds.email());
            registerForm.add("password", userCreds.password());

            Response register = AccountClient.register(resetExtension.anonApi(), registerForm);
            assertThat(register).hasStatusInfo(Response.Status.OK);
            userCookie = register.getCookies().get(FalloutService.COOKIE_NAME);
        }

        @Test
        public void test_auth_with_cookie()
        {
            assertThat(accountTarget().cookie(userCookie).get()).hasStatusInfo(Response.Status.OK);
        }
    }
}
