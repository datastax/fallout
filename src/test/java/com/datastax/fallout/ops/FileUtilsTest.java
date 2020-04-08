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
package com.datastax.fallout.ops;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.utils.FileUtils;

import static org.assertj.core.api.Assertions.assertThat;

public class FileUtilsTest extends TestHelpers.ArtifactTest
{
    private static final Logger classLogger = LoggerFactory.getLogger(FileUtilsTest.class);

    @Test
    public void compressGZIPCompressesFile() throws IOException
    {
        Path uncompressed = createTestRunArtifact("foo.log", "Hello\n");
        Path compressed = uncompressed.resolveSibling(uncompressed.getFileName() + ".gz");
        FileUtils.compressGZIP(uncompressed, compressed);

        assertThat(uncompressed.toFile()).doesNotExist();
        assertThat(compressed).exists();
    }

    @Test
    public void testGetSortedLogList() throws Exception
    {
        // Tests both getSortedDebugLogList() and unzipDebugLogs()
        Path path = getPath("log_zips");

        List<Path> expectedLogs = new ArrayList<>();
        expectedLogs.add(getPath("log_zips/debug.log.2017-06-20_1922"));
        expectedLogs.add(getPath("log_zips/debug.log.2017-06-22_2004"));
        expectedLogs.add(getPath("log_zips/debug.log.2017-06-24_1735"));
        expectedLogs.add(getPath("log_zips/debug.log"));

        List<Path> logFiles = FileUtils.getSortedLogList(path, "debug.log", classLogger, path);

        try
        {
            Assert.assertEquals("The logs were not correctly unzipped and sorted", expectedLogs, logFiles);
        }
        finally
        {
            for (Path log : logFiles)
            {
                if (!log.endsWith("debug.log"))
                {
                    Files.deleteIfExists(log);
                }
            }
        }
    }

    @Test
    public void testConcatenateBigLog() throws Exception
    {
        List<String> expectedBigLogLines = new ArrayList<>();
        for (int i = 0; i < 20; i++)
        {
            expectedBigLogLines.add(String.format("test %d", i));
        }

        List<Path> logsToConcat = new ArrayList<>();
        logsToConcat.add(getPath("unzipped_logs/debug.log.2017-06-20_1922"));
        logsToConcat.add(getPath("unzipped_logs/debug.log.2017-06-22_2004"));
        logsToConcat.add(getPath("unzipped_logs/debug.log.2017-06-24_1735"));
        logsToConcat.add(getPath("unzipped_logs/debug.log"));

        List<String> bigLogLines = FileUtils.concatenateBigLog(logsToConcat, classLogger);

        Assert.assertEquals("The logs were not read correctly", expectedBigLogLines, bigLogLines);
    }

    private Path getPath(String filePath)
    {
        return Paths.get("src/test/resources/data/repairtime/", filePath);
    }
}
