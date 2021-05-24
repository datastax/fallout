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

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.resources.AccountClient;
import com.datastax.fallout.service.resources.FalloutAppExtension;
import com.datastax.fallout.service.resources.RestApiBuilder;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public class AccountResourceTest extends WithFalloutAppExtension<FalloutAppExtension>
{
    private RestApiBuilder apiWithoutAuth;

    @RegisterExtension
    public static final FalloutAppExtension FALLOUT_SERVICE = new FalloutAppExtension();

    protected AccountResourceTest()
    {
        super(FALLOUT_SERVICE);
    }

    @BeforeEach
    public void setupRestApi()
    {
        apiWithoutAuth = getFalloutServiceResetExtension().anonApi();
    }

    @Test
    public void testRegister()
    {
        MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();

        formData.add("name", "user");
        formData.add("email", "usersitetld");
        formData.add("password", "123");

        // Create user with invalid email
        Response response = tryRegister(formData);
        assertThat(response).hasStatus(422);
        formData.putSingle("email", "user@site.tld");

        // Create user with blank name
        formData.putSingle("name", "");
        response = tryRegister(formData);
        assertThat(response).hasStatus(400);
        formData.putSingle("name", "user");

        // Create user with blank email
        formData.putSingle("email", "");
        response = tryRegister(formData);
        assertThat(response).hasStatus(400);
        formData.putSingle("email", "user@site.tld");

        // Create user with blank password
        formData.putSingle("password", "");
        response = tryRegister(formData);
        assertThat(response).hasStatus(400);
        formData.putSingle("password", "123");

        // Create valid user, check for 200
        response = tryRegister(formData);
        assertThat(response).hasStatus(200);

        // Check that 400 is returned when colliding
        // user registrations
        response = tryRegister(formData);
        assertThat(response).hasStatus(400);
    }

    @Test
    public void testLoginAndOut()
    {
        MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();

        formData.add("name", "user");
        formData.add("email", "user@site.tld");
        formData.add("password", "123");

        // Create valid user, check for 200
        Response response = tryRegister(formData);
        assertThat(response).hasStatus(200);

        response = tryLogin(formData.getFirst("email"), formData.getFirst("password"));
        assertThat(response).hasStatus(200);

        Cookie cookie = response.getCookies().get(FalloutService.COOKIE_NAME);

        response = tryLogout(cookie);
        assertThat(response).hasStatus(200);

        // Try invalid password
        response = tryLogin("user@site.tld", "456");
        assertThat(response).hasStatus(400);

        // Try blank password
        response = tryLogin("user@site.tld", "");
        assertThat(response).hasStatus(400);

        // Try blank username
        response = tryLogin("", "123");
        assertThat(response).hasStatus(400);

        // Clear form, fill with bad username
        response = tryLogin("notauser@site.tld", "123");
        assertThat(response).hasStatus(400);

        // Clear form, fill with valid login info
        response = tryLogin("user@site.tld", "123");
        assertThat(response).hasStatus(200);

        // Try connecting with valid user's cookie
        cookie = response.getCookies().get(FalloutService.COOKIE_NAME);
        response = tryGetAccount(cookie);
        assertThat(response).hasStatus(200);

        // Log out, then try logged out user's cookie
        response = tryLogout(cookie);
        assertThat(response).hasStatus(200);
        response = tryGetAccount(cookie);
        assertThat(response).hasStatus(401);
    }

    @Test
    public void testOauth()
    {
        MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();

        formData.add("name", "user");
        formData.add("email", "user@site.tld");
        formData.add("password", "123");

        // Create valid user, check for 200
        Response response = tryRegister(formData);
        assertThat(response).hasStatus(200);

        // Grab the oauthId from the C* table
        UUID oauthId = AccountClient.getOAuthId(FALLOUT_SERVICE.getSession(), "user@site.tld");

        // Check we can grab data via the Oauth token
        response = tryGetAccount(FalloutService.OAUTH_BEARER_TOKEN_TYPE + " " + oauthId);
        assertThat(response).hasStatus(200);

        // Verify that invalid oauth tokens don't return data
        response = tryGetAccount(FalloutService.OAUTH_BEARER_TOKEN_TYPE + " " + UUID.randomUUID());
        assertThat(response).hasStatus(401);
    }

    private Response tryLogin(String email, String password)
    {
        MultivaluedMap<String, String> formData = new MultivaluedHashMap<>();
        formData.add("email", email);
        formData.add("password", password);

        return apiWithoutAuth
            .target("/account/login")
            .request()
            .post(Entity.form(formData));
    }

    private Response tryLogout(Cookie cookie)
    {
        return apiWithoutAuth
            .target("/account/logout")
            .request()
            .cookie(cookie)
            .get();
    }

    private Response tryRegister(MultivaluedMap<String, String> formData)
    {
        return AccountClient.register(apiWithoutAuth, formData);
    }

    private Response tryGetAccount(String header)
    {
        return apiWithoutAuth
            .target("/account")
            .request()
            .header("Authorization", header)
            .get();
    }

    private Response tryGetAccount(Cookie cookie)
    {
        return apiWithoutAuth
            .target("/account")
            .request()
            .cookie(cookie)
            .get();
    }
}
