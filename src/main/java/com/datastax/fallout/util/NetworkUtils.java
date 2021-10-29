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
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

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
}
