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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;

public class NetworkUtils
{
    private static final int DOWNLOAD_TIMEOUT_MILLIS = 60_000;

    public static void downloadUrlToPath(URL url, Path localPath) throws IOException
    {
        FileUtils.copyURLToFile(url, localPath.toFile(),
            DOWNLOAD_TIMEOUT_MILLIS,
            DOWNLOAD_TIMEOUT_MILLIS);
    }

    public static byte[] downloadUrlAsBytes(URL url) throws IOException
    {
        URLConnection urlConnection = url.openConnection();
        urlConnection.setConnectTimeout(DOWNLOAD_TIMEOUT_MILLIS);
        urlConnection.setReadTimeout(DOWNLOAD_TIMEOUT_MILLIS);

        try (InputStream inputStream = urlConnection.getInputStream())
        {
            return inputStream.readAllBytes();
        }
    }

    public static String downloadUrlAsString(URL url) throws IOException
    {
        return new String(downloadUrlAsBytes(url), StandardCharsets.UTF_8);
    }

    public static URL parseUrl(String url)
    {
        try
        {
            return new URL(url);
        }
        catch (MalformedURLException e)
        {
            throw new IllegalArgumentException("Not a valid url: " + url, e);
        }
    }

    private static final int TIMEOUT_MILLIS = 60_000;

    public static String getAstraBundleUrl(String token, String dbID, String env) throws IOException
    {
        if (dbID.isEmpty() || env.isEmpty() || token.isEmpty())
        {
            throw new IllegalArgumentException("dbID, env, and token must all be set together");
        }

        // Convert env to uppercase
        String envValue = env.toUpperCase();
        String apiUrl;

        // Determine API base URL based on environment
        switch (envValue)
        {
            case "PROD":
                apiUrl = "https://api.astra.datastax.com";
                break;
            case "TEST":
                apiUrl = "https://api.test.cloud.datastax.com";
                break;
            case "DEV":
                apiUrl = "https://api.dev.cloud.datastax.com";
                break;
            default:
                throw new IllegalArgumentException("Unknown environment: " + env);
        }

        // Construct the API request URL
        String requestUrl = apiUrl + "/v2/databases/" + dbID + "/secureBundleURL?all=true";

        // Make the HTTP request and get the response
        String jsonResponse = makeHttpPostRequest(requestUrl, token);

        // Parse JSON response
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(jsonResponse);

        // Extract the top-level download URL
        JsonNode firstEntry = rootNode.get(0); // Assuming the first entry is relevant

        if (firstEntry != null && firstEntry.has("downloadURL"))
        {
            return firstEntry.get("downloadURL").asText();
        }
        else
        {
            return "";
        }
    }

    private static String makeHttpPostRequest(String requestUrl, String token) throws IOException
    {
        URL url = new URL(requestUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setConnectTimeout(TIMEOUT_MILLIS);
        connection.setReadTimeout(TIMEOUT_MILLIS);
        connection.setRequestProperty("Authorization", "Bearer " + token);
        connection.setRequestProperty("Accept", "application/json");

        // Read the response
        try (Scanner scanner = new Scanner(connection.getInputStream(), StandardCharsets.UTF_8))
        {
            return scanner.useDelimiter("\\A").next();
        }
    }
}
