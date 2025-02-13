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

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of HTTP related utility methods.
 */
public class HttpUtils
{
    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    public static boolean httpPostJson(String uri, String jsonPayload)
    {
        HttpPost postEventRequest = new HttpPost(uri);
        postEventRequest.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON));
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            CloseableHttpResponse rawResponse = httpClient.execute(postEventRequest))
        {
            return rawResponse.getStatusLine().getStatusCode() == 200;
        }
        catch (IOException e)
        {
            log.error("Exception during json post: ", e);
            return false;
        }
    }

    public static Optional<JsonNode> httpGetJson(String uri, Optional<Logger> opLogger)
    {
        return httpGetJson(uri, opLogger, Optional.empty());
    }

    public static Optional<JsonNode> httpGetJson(String uri, Optional<Logger> opLogger, Optional<String> authToken)
    {
        Logger logger = opLogger.orElse(log);
        String jsonStr = "";
        try
        {
            jsonStr = httpGetString(uri, authToken);
        }
        catch (IOException e)
        {
            logger.error("Failed to fetch json URI: " + uri, e);
            return Optional.empty();
        }

        logger.info("Parsing response: " + jsonStr);
        try
        {
            return Optional.of(JsonUtils.getJsonNode(jsonStr));
        }
        catch (Exception e)
        {
            logger.error("Failed to parse json response: " + jsonStr, e);
            return Optional.empty();
        }
    }

    public static String httpGetString(String uri, Optional<String> authToken) throws IOException
    {
        HttpGet postGetRequest = new HttpGet(uri);
        if (!authToken.isEmpty())
        {
            postGetRequest.setHeader("Authorization", "Bearer " + authToken);
        }
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build();
            CloseableHttpResponse rawResponse = httpClient.execute(postGetRequest))
        {
            int code = rawResponse.getStatusLine().getStatusCode();
            if (code != 200)
            {
                throw new IOException(
                    "HTTP Get request failed with " + code + ":  " + rawResponse.getStatusLine().getReasonPhrase());
            }
            try (var inputStream = rawResponse.getEntity().getContent())
            {
                return FileUtils.readString(inputStream);
            }
        }
    }
}
