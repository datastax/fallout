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
package com.datastax.fallout.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import io.dropwizard.testing.ConfigOverride;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.datastax.fallout.service.resources.FalloutAppExtension;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.service.FalloutConfiguration.ServerMode.RUNNER;
import static org.awaitility.Awaitility.await;

@Tag("requires-db")
public class FalloutServiceLoggingTest extends WithPersistentTestOutputDir
{
    private static final String APP_LOG_FILENAME = "fallout.log";

    /**
     * we do not use {@link org.junit.jupiter.api.extension.RegisterExtension} here: we want to start {@link FalloutAppExtension} manually
     **/
    private FalloutAppExtension falloutService;

    private Path falloutYml;

    @BeforeEach
    public void createFalloutYml() throws IOException
    {
        falloutYml = persistentTestOutputDir().resolve("fallout.yml");
        Files.write(falloutYml, List.of(
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

        falloutService = new FalloutAppExtension(mode, falloutYml.toString(), overrides_);

        Exceptions.runUnchecked(() -> falloutService.before(persistentTestOutputDir()));
    }

    private void startService(FalloutConfiguration.ServerMode mode, String directory)
    {
        startService(mode,
            ConfigOverride.config("logging.appenders[0].currentLogFilename", directory + APP_LOG_FILENAME));
    }

    @AfterEach
    public void teardown()
    {
        falloutService.after();
    }

    private Path logPath(String logDir)
    {
        return persistentTestOutputDir().resolve(logDir);
    }

    private Path logPath(FalloutConfiguration.ServerMode mode, String logFilename)
    {
        return mode == RUNNER ?
            logPath("logs/runners/1/" + logFilename) :
            logPath("logs/" + logFilename);
    }

    static Stream<Arguments> modesAndDirectories()
    {
        return Arrays.stream(FalloutConfiguration.ServerMode.values())
            .flatMap(mode -> Stream.of("foo/bar/", "")
                .map(dir -> Arguments.arguments(mode, dir)));
    }

    @ParameterizedTest
    @MethodSource("modesAndDirectories")
    public void appender_directory_is_ignored_and_runner_mode_uses_a_different_directory(
        FalloutConfiguration.ServerMode serverMode, String appenderDirectory)
    {
        startService(serverMode, appenderDirectory);
        await().untilAsserted(() -> assertThat(logPath(serverMode, APP_LOG_FILENAME)).exists());
    }
}
