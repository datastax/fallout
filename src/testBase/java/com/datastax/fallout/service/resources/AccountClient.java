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
package com.datastax.fallout.service.resources;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.ResponseAssert;

import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.Session;

import static javax.ws.rs.core.Response.Status.OK;
import static org.assertj.core.api.Assertions.assertThat;

public class AccountClient
{
    private final Session session;
    private final RestApiBuilder anonApi;

    public AccountClient(Session session, RestApiBuilder anonApi)
    {
        this.session = session;
        this.anonApi = anonApi;
    }

    public static UUID register(Session session, RestApiBuilder api, String email)
    {
        assertThat(email).isNotNull();
        String[] emailSplit = email.split("@");
        assertThat(emailSplit).hasSize(2);

        ResponseAssert.assertThat(register(api, new MultivaluedHashMap<>(Map.of(
            "name", emailSplit[0],
            "email", email,
            "password", "123")))).hasStatusInfo(OK);

        return getOAuthId(session, email);
    }

    public static Response register(RestApiBuilder api, MultivaluedMap<String, String> formData)
    {
        return api
            .target("/account/register")
            .request()
            .post(Entity.form(formData));
    }

    public static UUID getOAuthId(Session session, String user)
    {
        return session.execute("SELECT oauthId FROM users WHERE email=?", user)
            .one().getUUID(0);
    }

    public UUID register(String email)
    {
        return register(session, anonApi, email);
    }

    public UUID registerAdmin(String email)
    {
        final var oAuthId = register(email);
        session.execute("UPDATE users SET admin=true WHERE email=?", email);
        return oAuthId;
    }
}
