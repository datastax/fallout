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
package com.datastax.fallout.ops.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import com.google.common.base.Verify;
import org.slf4j.Logger;

import com.datastax.fallout.util.Exceptions;

public class FileUtils
{
    public static void compressGZIP(Path input, Path output) throws IOException
    {
        try (GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(output)))
        {
            Files.copy(input, out);
        }
    }

    public static void uncompressGZIP(Path input, Path output) throws IOException
    {
        try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(input)))
        {
            Files.copy(in, output);
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
                    writeString(zipEntryPath, result);

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

    public static void unzipArchive(Path archive, Path output, Logger logger)
    {
        // taken largely from https://www.baeldung.com/java-compress-and-uncompress
        try
        {
            ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(archive.toFile()));
            ZipEntry zipEntry = zipInputStream.getNextEntry();
            byte[] buffer = new byte[1024];
            while (zipEntry != null)
            {
                File newFile = new File(output.toFile(), zipEntry.getName());
                if (zipEntry.isDirectory())
                {
                    if (!newFile.isDirectory() && !newFile.mkdirs())
                    {
                        throw new IOException("Failed to create directory " + newFile);
                    }
                }
                else
                {
                    // fix for Windows-created archives
                    File parent = newFile.getParentFile();
                    if (!parent.isDirectory() && !parent.mkdirs())
                    {
                        throw new IOException("Failed to create directory " + parent);
                    }

                    // write file content
                    FileOutputStream fos = new FileOutputStream(newFile);
                    int len;
                    while ((len = zipInputStream.read(buffer)) > 0)
                    {
                        fos.write(buffer, 0, len);
                    }
                    fos.close();
                }
                zipEntry = zipInputStream.getNextEntry();
            }
        }
        catch (IOException e)
        {
            logger.error(String.format("Failed to unzip archive %s.", archive), e);
        }
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

    public static Path createDirs(Path dir)
    {
        return Exceptions.getUncheckedIO(() -> Files.createDirectories(dir)
        );
    }

    public static void deleteDir(Path dir)
    {
        Exceptions.runUncheckedIO(() -> org.apache.commons.io.FileUtils.deleteDirectory(dir.toFile())
        );
    }

    public static List<Path> listDir(Path dir)
    {
        try (Stream<Path> paths = Exceptions.getUncheckedIO(() -> Files.list(dir)))
        {
            // finalize stream so that we can close the open directory
            return paths.collect(Collectors.toList());
        }
    }

    public static void writeString(Path file, String content)
    {
        Exceptions.runUncheckedIO(() -> Files.writeString(file, content));
    }

    public static String readString(Path file)
    {
        return Exceptions.getUncheckedIO(() -> Files.readString(file));
    }
}
