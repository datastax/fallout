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
package com.datastax.fallout.service.resources.server;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import java.util.UUID;

import io.dropwizard.setup.Environment;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.resources.AccountClient;
import com.datastax.fallout.service.resources.FalloutServiceRule;
import com.datastax.fallout.test.utils.categories.RequiresDb;

import static com.datastax.fallout.service.FalloutConfiguration.AuthenticationMode.SINGLE_USER;
import static javax.ws.rs.core.ResponseAssert.assertThat;

@Category(RequiresDb.class)
public abstract class AuthTest
{
    static final String userCredsString = "user:email@email.com:password";
    static final FalloutConfiguration.UserCreds userCreds = FalloutConfiguration.UserCreds.from(userCredsString);

    abstract FalloutServiceRule getServiceRule();

    abstract FalloutServiceRule.FalloutServiceResetRule getResetRule();

    Invocation.Builder accountTarget()
    {
        return getResetRule().anonApi().target("/account").request();
    }

    @Test
    public void test_auth_using_oauth_token()
    {
        UUID oauthToken = AccountClient.getOAuthId(getServiceRule().getSession(), userCreds.getEmail());
        String header = String.format("%s %s", FalloutService.OAUTH_BEARER_TOKEN_TYPE, oauthToken);
        assertThat(accountTarget().header("Authorization", header).get()).hasStatusInfo(Response.Status.OK);
    }

    public static class SingleUser extends AuthTest
    {
        @ClassRule
        public static final FalloutServiceRule falloutServiceRule;

        @Rule
        public final FalloutServiceRule.FalloutServiceResetRule resetRule =
            falloutServiceRule.resetRule(FalloutServiceRule.ResetBehavior.DO_NOT_CLEAN_DATABASE);

        static
        {
            falloutServiceRule = new FalloutServiceRule();
            falloutServiceRule.addListener(new DropwizardAppRule.ServiceListener<>()
            {
                @Override
                public void onRun(FalloutConfiguration configuration, Environment environment,
                    DropwizardAppRule<FalloutConfiguration> rule)
                {
                    configuration.setAdminUserCreds(userCredsString);
                    configuration.setAuthenticationMode(SINGLE_USER);
                }
            });
        }

        @Override
        protected FalloutServiceRule getServiceRule()
        {
            return falloutServiceRule;
        }

        @Override
        FalloutServiceRule.FalloutServiceResetRule getResetRule()
        {
            return resetRule;
        }

        @Test
        public void test_auth_without_cookie()
        {
            assertThat(accountTarget().get()).hasStatusInfo(Response.Status.OK);
        }
    }

    public static class MultiUser extends AuthTest
    {
        @ClassRule
        public static final FalloutServiceRule falloutServiceRule = new FalloutServiceRule();

        @Rule
        public final FalloutServiceRule.FalloutServiceResetRule resetRule = falloutServiceRule.resetRule();

        private static Cookie userCookie;

        @Override
        protected FalloutServiceRule getServiceRule()
        {
            return falloutServiceRule;
        }

        @Override
        FalloutServiceRule.FalloutServiceResetRule getResetRule()
        {
            return resetRule;
        }

        @Before
        public void registerAccount()
        {
            MultivaluedMap<String, String> registerForm = new MultivaluedHashMap<>();
            registerForm.add("name", userCreds.getName());
            registerForm.add("email", userCreds.getEmail());
            registerForm.add("password", userCreds.getPassword());

            Response register = AccountClient.register(resetRule.anonApi(), registerForm);
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
