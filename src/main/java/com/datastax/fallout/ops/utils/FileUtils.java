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
package com.datastax.fallout.ops.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.common.base.Verify;
import org.slf4j.Logger;

import com.datastax.fallout.ops.Utils;

public class FileUtils
{
    /**
     * Compresses a file in gzip format.
     */
    public static void compressGZIP(Path input, Path output) throws IOException
    {
        try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(output)))
        {
            Files.copy(input, out);
            Files.delete(input);
        }
    }

    /**
     * @param logPath - directory to begin looking for fileName
     * @param fileName - name of the log being searched for IE: "system.log"
     * @param logger
     * @param outputDir - directory where unzipped log files should be written
     * @return a list of logs matching the fileName, sorted by age
     */
    public static List<Path> getSortedLogList(Path logPath, String fileName, Logger logger, Path outputDir)
    {
        List<Path> logsInPath = null;
        try
        {
            logsInPath = Files.walk(logPath).filter(f -> f.endsWith(fileName)).collect(Collectors.toList());
        }
        catch (IOException e)
        {
            logger.error("No files found", e);
        }
        Verify.verifyNotNull(logsInPath);

        List<Path> unzippedLogs = unzipLogs(logPath, fileName, logger, outputDir);
        Collections.sort(unzippedLogs);
        unzippedLogs.addAll(logsInPath);

        return unzippedLogs;
    }

    private static List<Path> unzipLogs(Path logPath, String fileName, Logger logger, Path outputDir)
    {
        List<Path> zippedLogsInPath = null;
        List<Path> unzippedLogs = new ArrayList<>();
        try
        {
            zippedLogsInPath = Files.walk(logPath)
                .filter(f -> f.toString().toLowerCase().contains(fileName))
                .filter(f -> f.toString().toLowerCase().contains(".zip"))
                .collect(Collectors.toList());
        }
        catch (IOException e)
        {
            logger.error("No files found");
        }

        Verify.verifyNotNull(zippedLogsInPath);
        for (Path filePath : zippedLogsInPath)
        {
            try
            {
                ZipFile zipFile = new ZipFile(filePath.toString());
                if (zipFile.size() > 1)
                {
                    throw new IOException(
                        "Only one file is expected from an unzipped log. More than one file was found.");
                }
                else
                {
                    Enumeration<? extends ZipEntry> entries = zipFile.entries();
                    ZipEntry zipEntry = entries.nextElement();
                    Path zipEntryPath = Paths.get(outputDir.toString(), zipEntry.getName());

                    InputStream inputStream = zipFile.getInputStream(zipEntry);
                    String result = new BufferedReader(new InputStreamReader(inputStream)).lines()
                        .collect(Collectors.joining("\n"));
                    Utils.writeStringToFile(zipEntryPath.toFile(), result);

                    unzippedLogs.add(zipEntryPath);
                }
            }
            catch (IOException e)
            {
                logger.error("There was a problem opening the zip file", e);
            }
        }
        return unzippedLogs;
    }

    public static List<String> concatenateBigLog(List<Path> logList, Logger logger)
    {
        List<String> myBigLog = new ArrayList<>();
        for (Path log : logList)
        {
            try
            {
                myBigLog.addAll(Files.readAllLines(log));
            }
            catch (IOException e)
            {
                logger.error("There was a problem reading the log file", e);
            }
        }
        return myBigLog;
    }
}
