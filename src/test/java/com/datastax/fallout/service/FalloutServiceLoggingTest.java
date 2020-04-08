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
package com.datastax.fallout.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import io.dropwizard.testing.ConfigOverride;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.datastax.fallout.service.resources.FalloutServiceRule;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;
import com.datastax.fallout.test.utils.categories.RequiresDb;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.service.FalloutConfiguration.ServerMode.RUNNER;
import static com.datastax.fallout.service.FalloutConfiguration.ServerMode.STANDALONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Category(RequiresDb.class)
public class FalloutServiceLoggingTest extends WithPersistentTestOutputDir
{
    // Deliberately _not_ using @Rule: we want to set this up and tear it down for each test
    private Optional<FalloutServiceRule> falloutServiceClassRule = Optional.empty();

    private Path falloutYml;

    @Before
    public void createFalloutYml() throws IOException
    {
        falloutYml = persistentTestOutputDir().resolve("fallout.yml");
        Files.write(falloutYml, ImmutableList.of(
            "logging:",
            "  level: INFO",
            "  appenders:",
            "    - type: file",
            "      archive: false",
            "      currentLogFilename: fallout.log",
            "    - type: console"
        ), StandardCharsets.UTF_8);
    }

    private void startService(FalloutConfiguration.ServerMode mode, ConfigOverride... overrides)
    {
        ConfigOverride[] overrides_ = ImmutableList.<ConfigOverride>builder()
            .add(overrides)
            .add(ConfigOverride.config("falloutHome", persistentTestOutputDir().toString()))
            .build()
            .toArray(new ConfigOverride[0]);

        final FalloutServiceRule falloutServiceClassRule =
            FalloutServiceRule.withoutRuleAnnotation(mode,
                persistentTestOutputDir(),
                falloutYml.toString(),
                overrides_);

        Exceptions.runUnchecked(falloutServiceClassRule::before);

        this.falloutServiceClassRule = Optional.of(falloutServiceClassRule);
    }

    private void startService(FalloutConfiguration.ServerMode mode, String logFile)
    {
        startService(mode,
            ConfigOverride.config("logging.appenders[0].currentLogFilename", logFile));
    }

    @After
    public void teardown()
    {
        falloutServiceClassRule.ifPresent(FalloutServiceRule::after);
    }

    private Path logPath(String logDir)
    {
        return persistentTestOutputDir().resolve(logDir);
    }

    @Test
    public void standalone_mode_ignores_appender_directory()
    {
        startService(STANDALONE, "foo/bar/fallout.log");
        await().untilAsserted(() -> assertThat(logPath("logs/fallout.log")).exists());
    }

    @Test
    public void runner_mode_ignores_appender_directory()
    {
        startService(RUNNER, "foo/bar/fallout.log");
        await().untilAsserted(() -> assertThat(logPath("logs/runners/1/fallout.log")).exists());
    }

    @Test
    public void runner_mode_appends_runner_id_to_default_path()
    {
        startService(RUNNER, "fallout.log");
        await().untilAsserted(() -> assertThat(logPath("logs/runners/1/fallout.log")).exists());
    }
}
