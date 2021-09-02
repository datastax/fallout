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
package com.datastax.fallout.service.views;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import io.dropwizard.views.View;

import com.datastax.fallout.service.core.User;
import com.datastax.fallout.util.DateUtils;
import com.datastax.fallout.util.JsonUtils;

public class FalloutView extends View
{
    protected final User user;
    protected final Date generationDate;
    private final MainView mainView;
    private final String title;

    public FalloutView(List<String> titleParts, String templateName, User user, MainView mainView)
    {
        super("/" + FalloutView.class.getPackage().getName().replace('.', '/') + "/" + templateName);
        this.title = titleParts.stream()
            .filter(s -> !s.isBlank())
            .map(mainView::hideDisplayedEmailDomains)
            .collect(Collectors.joining(" | "));
        this.user = user;
        this.generationDate = new Date();
        this.mainView = mainView;
    }

    public FalloutView(List<String> titleParts, String templateName, Optional<User> user, MainView mainView)
    {
        this(titleParts, templateName, user.orElse(null), mainView);
    }

    public final User getUser()
    {
        return user;
    }

    public final Date getGenerationDate()
    {
        return generationDate;
    }

    public final String getGenerationDateUtc()
    {
        return DateUtils.formatUTCDate(getGenerationDate());
    }

    public static final Response error(String error)
    {
        return Response.status(Response.Status.BAD_REQUEST)
            .entity(String.format("{\"error\": %s}", JsonUtils.toJson(error)))
            .build();
    }

    public static final Response error(Throwable e)
    {
        StringWriter w = new StringWriter();
        PrintWriter p = new PrintWriter(w);
        e.printStackTrace(p);

        return error(w.toString());
    }

    public static URI uriFor(Class clazz, String method, Object... params)
    {
        return UriBuilder.fromResource(clazz).path(clazz, method).build(params);
    }

    public static URI uriFor(Class clazz)
    {
        return UriBuilder.fromResource(clazz).build();
    }

    public static String linkFor(String content, Class clazz, String method, Object... params)
    {
        final String href = uriFor(clazz, method, params).toString();
        return "<a href=\"" + href + "\">" + content + "</a>";
    }

    public static String linkFor(String content, URI uri)
    {
        final String href = uri.toString();
        return "<a href=\"" + href + "\">" + content + "</a>";
    }

    public MainView getMainView()
    {
        return mainView;
    }

    public String getTitle()
    {
        return title;
    }
}
