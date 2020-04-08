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
package com.datastax.fallout;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import ch.qos.logback.classic.BasicConfigurator;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.Configurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.google.auto.service.AutoService;
import io.dropwizard.testing.ConfigOverride;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.ops.JobFileLoggers;

/** Override logback pattern and levels in tests using slf4j static initialisation.
 *
 *  Logback looks for implementations of Configurator using {@link java.util.ServiceLoader#load(Class)}, and uses
 *  those to configure the main LoggerContext (see implementation of
 *  {@link ch.qos.logback.classic.util.ContextInitializer#autoConfig}).
 *
 *  This also looks for system properties of the form log.LOGGER-NAME.level=LEVEL, and
 *  sets LOGGER-NAME to LEVEL; see {@link #logLevelsFromSystemProperties()} for details.
 */
@AutoService(Configurator.class)
public class TestLogbackConfigurator extends BasicConfigurator
{
    private static final Pattern LOG_LEVEL_PROPERTY = Pattern.compile("log\\.(?:([\\d\\w.]*)\\.)?level");

    private static Stream<Pair<String, Level>> logLevelsFromSystemProperties()
    {
        return System.getProperties().stringPropertyNames().stream()
            .map(LOG_LEVEL_PROPERTY::matcher)
            .filter(Matcher::matches)
            .map(matcher -> Pair.of(
                matcher.group(1) == null ? Logger.ROOT_LOGGER_NAME : matcher.group(1),
                Level.valueOf(System.getProperty(matcher.group()))));
    }

    private static Stream<Pair<String, Level>> defaultLogLevels()
    {
        return Stream.of(
            Pair.of(Logger.ROOT_LOGGER_NAME, Level.INFO),
            Pair.of("com.datastax.fallout", Level.DEBUG),
            Pair.of("com.datastax.fallout.cassandra.shaded", Level.INFO),
            Pair.of("com.datastax.driver", Level.WARN),
            Pair.of("org.apache.cassandra", Level.WARN),
            Pair.of("org.eclipse.jetty.server", Level.WARN));
    }

    private static void setLogLevels(LoggerContext lc, Stream<Pair<String, Level>> loggerLevels)
    {
        loggerLevels
            .forEach(loggerLevel -> lc.getLogger(loggerLevel.getLeft()).setLevel(loggerLevel.getRight()));
    }

    /** Dropwizard resets all loggers within DefaultLoggingFactory's initialisation (see
     * {@link io.dropwizard.logging.DefaultLoggingFactory#configureLoggers()}; this method
     * allows us to extract the common logging settings for testing as {@link ConfigOverride}s. */
    public static Stream<ConfigOverride> getConfigOverrides()
    {
        return Stream
            .concat(
                defaultLogLevels(),
                logLevelsFromSystemProperties())
            .map(loggerLevel -> ConfigOverride.config(
                "logging.loggers." + loggerLevel.getLeft().replace(".", "\\."),
                loggerLevel.getRight().levelStr));
    }

    @Override
    public void configure(LoggerContext lc)
    {
        final ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<ILoggingEvent>();
        appender.setContext(lc);
        appender.setName("console");

        final PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern(JobFileLoggers.FALLOUT_PATTERN);
        encoder.setContext(lc);
        encoder.start();

        appender.setEncoder(encoder);
        appender.start();

        final Logger rootLogger = lc.getLogger(Logger.ROOT_LOGGER_NAME);
        rootLogger.addAppender(appender);

        rootLogger.setLevel(Level.INFO);

        setLogLevels(lc, Stream.concat(defaultLogLevels(), logLevelsFromSystemProperties()));
    }
}
