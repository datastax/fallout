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
package com.datastax.fallout.service.auth;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.SecurityContext;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import io.dropwizard.auth.AuthFilter;

import com.datastax.fallout.service.core.User;

public class SingleUserAuthFilter extends AuthFilter<String, User>
{
    public SingleUserAuthFilter(Supplier<User> defaultAdmin)
    {
        authenticator = ignored -> Optional.of(defaultAdmin.get());
    }

    @Override
    public void filter(ContainerRequestContext containerRequestContext) throws IOException
    {
        authenticate(containerRequestContext, "", SecurityContext.BASIC_AUTH);
    }
}
