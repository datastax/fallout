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
package com.datastax.fallout.service.resources;

import javax.ws.rs.client.Client;

import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import org.glassfish.jersey.client.oauth2.OAuth2ClientSupport;

import com.datastax.driver.core.Session;
import com.datastax.fallout.service.FalloutClientBuilder;

import static com.datastax.fallout.service.core.Fakes.TEST_USER_EMAIL;

public class FalloutClient
{
    private Supplier<RestApiBuilder> anonApiCreator;
    private Supplier<RestApiBuilder> userApiCreator;
    private Supplier<RestApiBuilder> corruptUserApiCreator;
    private Supplier<RestApiBuilder> adminApiCreator;

    private final Supplier<AccountClient> accountClientCreator;

    public FalloutClient(URI falloutServiceUri, Supplier<Session> sessionProvider)
    {
        anonApiCreator = Suppliers.memoize(() -> new RestApiBuilder(
            () -> createClient("anon"),
            falloutServiceUri));

        this.accountClientCreator =
            Suppliers.memoize(() -> new AccountClient(sessionProvider.get(), anonApiCreator.get()));

        userApiCreator = Suppliers.memoize(() -> new RestApiBuilder(
            () -> createClient("user", this.accountClientCreator.get().register(TEST_USER_EMAIL)),
            falloutServiceUri));

        corruptUserApiCreator = Suppliers.memoize(() -> new RestApiBuilder(
            () -> createClient(
                "corrupt", this.accountClientCreator.get().registerCorruptUser("corrupt@example.com")),
            falloutServiceUri));

        final var adminUser = "fallout-admin@example.com";

        adminApiCreator = Suppliers.memoize(() -> new RestApiBuilder(
            () -> createClient("admin", this.accountClientCreator.get().registerAdmin(adminUser)),
            falloutServiceUri));
    }

    private Client createClient(String name)
    {
        return FalloutClientBuilder.named(name).build();
    }

    private Client createClient(String name, UUID oAuthId)
    {
        return createClient(name).register(OAuth2ClientSupport.feature(oAuthId.toString()));
    }

    public RestApiBuilder anonApi()
    {
        return anonApiCreator.get();
    }

    public RestApiBuilder userApi()
    {
        return userApiCreator.get();
    }

    public RestApiBuilder corruptUserApi()
    {
        return corruptUserApiCreator.get();
    }

    public RestApiBuilder adminApi()
    {
        return adminApiCreator.get();
    }
}
