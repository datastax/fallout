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
package com.datastax.fallout.util;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlackUserMessenger implements UserMessenger
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(SlackUserMessenger.class);

    private Map<String, String> slackUserIDs = new HashMap<>();
    private final Client httpClient;
    private final String slackToken;

    public static UserMessenger create(String slackToken, Client httpClient)
    {
        final Logger log = LoggerFactory.getLogger(SlackUserMessenger.class);
        if (slackToken != null)
        {
            return new SlackUserMessenger(httpClient, slackToken);
        }
        else
        {
            log.warn("Fallout config missing slackToken");
            return new NullUserMessenger();
        }
    }

    private SlackUserMessenger(Client httpClient, String slackToken)
    {
        this.httpClient = httpClient;
        this.slackToken = slackToken;
    }

    private String sanitizedSlackUri(URI slackUri)
    {
        return slackUri.toString().replace(slackToken, "<secret>");
    }

    private UriBuilder slackUriBuilder(String method)
    {
        return UriBuilder.fromPath("https://slack.com")
            .path("/api/" + method)
            .queryParam("token", slackToken);
    }

    /**
     *  'channel' is the Slack naming for this value, which can be a number of different things.
     *  In this implementation we return the current user's Slack user ID -- for sending them a Direct Message.
     */
    private String getSlackChannel(String email) throws MessengerException
    {
        String result = slackUserIDs.get(email);
        if (result != null)
        {
            return result;
        }

        final URI slackUri = slackUriBuilder("users.lookupByEmail")
            .queryParam("email", email)
            .build();

        JsonNode json;

        try
        {
            json = httpClient.target(slackUri).request()
                .accept(MediaType.APPLICATION_JSON)
                .get(JsonNode.class);
        }
        catch (Exception e)
        {
            throw new MessengerException(String.format("GET %s failed", sanitizedSlackUri(slackUri)), e);
        }

        String userId;
        if (json.path("ok").asText().equals("true") &&
            !(userId = json.at("/user/id").asText()).isEmpty())
        {
            slackUserIDs.put(email, userId);
            return userId;
        }
        else
        {
            throw new MessengerException(String.format("GET %s returned unexpected JSON: %s",
                sanitizedSlackUri(slackUri),
                json));
        }
    }

    @Override
    public void sendMessage(String email, String subject, String body) throws MessengerException
    {
        logger.withScopedDebug("sendMessage({}, {}, {})", email, subject, body).run(() -> {
            String channel =
                logger.withScopedDebug("getSlackChannel({})", email).get(() -> getSlackChannel(email));

            final URI slackUri = slackUriBuilder("chat.postMessage")
                .queryParam("channel", channel)
                .queryParam("text", body)
                .build();

            JsonNode json;

            try
            {
                json = httpClient.target(slackUri).request()
                    .post(null, JsonNode.class);
            }
            catch (Exception e)
            {
                throw new MessengerException(String.format("POST %s failed", sanitizedSlackUri(slackUri)), e);
            }

            if (!json.path("ok").asText().equals("true"))
            {
                throw new MessengerException(String.format("POST %s returned unexpected JSON: %s",
                    sanitizedSlackUri(slackUri),
                    json));
            }
        });
    }
}
