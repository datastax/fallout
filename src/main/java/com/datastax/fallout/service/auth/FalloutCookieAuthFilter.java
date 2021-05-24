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

import javax.annotation.Nullable;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.SecurityContext;

import java.io.IOException;

import io.dropwizard.auth.AuthFilter;

import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.core.User;

@Priority(Priorities.AUTHENTICATION)
public class FalloutCookieAuthFilter extends AuthFilter<String, User>
{

    private FalloutCookieAuthFilter()
    {
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException
    {
        final String cookie = readAuthCookie(requestContext);
        if (!authenticate(requestContext, cookie, SecurityContext.BASIC_AUTH))
        {
            throw new WebApplicationException(unauthorizedHandler.buildResponse(prefix, realm));
        }
    }

    @Nullable
    private String readAuthCookie(ContainerRequestContext request)
    {
        if (request.getCookies() != null)
        {
            Cookie cookie = request.getCookies().get(FalloutService.COOKIE_NAME);
            if (cookie != null)
            {
                return cookie.getValue();
            }
        }
        return null;
    }

    public static class Builder extends AuthFilterBuilder<String, User, FalloutCookieAuthFilter>
    {

        @Override
        protected FalloutCookieAuthFilter newInstance()
        {
            return new FalloutCookieAuthFilter();
        }
    }
}
