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
package com.datastax.fallout.test.utils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.ops.utils.FileUtils;

public class TestOutputDir
{
    private final static Logger logger = LoggerFactory.getLogger(TestOutputDir.class);

    public static final Path FALLOUT_TESTS_DIR = Paths.get("build/fallout-tests");
    private volatile static Path testBaseOutputDir = null;
    private volatile static String testStartDate = null;

    public static Path getTestBaseOutputDir()
    {
        setTestBaseOutputDir();
        return testBaseOutputDir;
    }

    static void setTestBaseOutputDir()
    {
        if (testStartDate == null)
        {
            testStartDate = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH_mm_ss"));
            logger.info("testStartDate: {}", testStartDate);

            testBaseOutputDir = FALLOUT_TESTS_DIR.resolve(testStartDate).toAbsolutePath();
            FileUtils.createDirs(testBaseOutputDir);
            logger.info("testBaseOutputDir: {}", testBaseOutputDir);
        }
    }
}
