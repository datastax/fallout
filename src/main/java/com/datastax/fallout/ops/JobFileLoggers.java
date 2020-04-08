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

import java.io.Closeable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.OutputStreamAppender;
import com.google.common.collect.ImmutableList;

public class JobFileLoggers implements Closeable, JobLoggers
{
    private final Path base;
    private final Logger shared;

    /**
     * Rather than creating new loggers with SLF4J's {@link org.slf4j.LoggerFactory#getLogger}, we use
     * an explicit {@link ch.qos.logback.classic.LoggerContext} per {@link JobLoggers}.  This has two effects:
     *
     * <ul>
     *
     * <li> Most importantly, any loggers created belong to the context's root logger instead of the default root
     *   logger.  Since the lifetime of the default root logger is that of the application, its child loggers
     *   will also live that long; fallout creates many unique loggers during its lifetime, which would eventually
     *   exhaust all memory.  By using our own context, we can dispose of its loggers along with the Factory.
     *
     * <li> The context also provides a namespace for the loggers it creates, so we
     *   don't need to go to any effort to create uniquely named loggers per test run.
     *
     * </ul>
     */
    private final LoggerContext loggerContext;
    private final PatternLayoutEncoder patternLayoutEncoder;

    public JobFileLoggers(Path base, boolean logTestRunsToConsole)
    {
        loggerContext = new LoggerContext();
        loggerContext.start();

        patternLayoutEncoder = new PatternLayoutEncoder();
        patternLayoutEncoder.setContext(loggerContext);
        patternLayoutEncoder.setPattern(FALLOUT_PATTERN);
        patternLayoutEncoder.start();

        this.base = base;
        this.shared = createSharedLogger(logTestRunsToConsole);
    }

    public org.slf4j.Logger create(String name, Path relFile)
    {
        return createLogger(SHARED_LOGGER_NAME + "." + name,
            ImmutableList.of(withThresholdFilter(createFileAppender(name, base.resolve(relFile)), Level.INFO)));
    }

    @Override
    public org.slf4j.Logger getShared()
    {
        return shared;
    }

    /**
     * Closes all loggers created by this factory instance
     */
    @Override
    public void close()
    {
        loggerContext.stop();
    }

    private static final String THREAD_PATTERN =
        "%-21replace(" +
            "%replace([%thread]){'ForkJoinPool\\.commonPool-worker-', 'FJP:'}" +
            "){'clojure-agent-send-off-pool-', 'clojure:'}";
    public static final String FALLOUT_PATTERN =
        "%-41(%date " +
            THREAD_PATTERN +
            " %-5level %-20logger{0}) - %msg%n";

    private OutputStreamAppender<ILoggingEvent> withThresholdFilter(OutputStreamAppender<ILoggingEvent> appender,
        Level level)
    {
        final ThresholdFilter filter = new ThresholdFilter();
        filter.setContext(loggerContext);
        filter.setLevel(level.levelStr);
        filter.start();

        appender.addFilter(filter);

        return appender;
    }

    private OutputStreamAppender<ILoggingEvent> createFileAppender(String name, Path logFilePath)
    {
        final FileAppender<ILoggingEvent> fileAppender = new FileAppender<>();
        fileAppender.setFile(logFilePath.toFile().getAbsolutePath());
        fileAppender.setName(name);
        fileAppender.setContext(loggerContext);
        fileAppender.setEncoder(patternLayoutEncoder);
        fileAppender.start();

        return fileAppender;
    }

    private OutputStreamAppender<ILoggingEvent> createSharedLevelFileAppender(String suffix, Level level)
    {
        return withThresholdFilter(createFileAppender(
            SHARED_LOGGER_NAME + suffix, base.resolve(SHARED_LOGGER_NAME + suffix + ".log")), level);
    }

    private static final String SHARED_LOGGER_NAME = "fallout";

    private Logger createSharedLogger(boolean logTestRunsToConsole)
    {
        List<OutputStreamAppender<ILoggingEvent>> appenders = new ArrayList<>(5);
        appenders.add(withThresholdFilter(createFileAppender(
            SHARED_LOGGER_NAME, base.resolve(SHARED_LOGGER_NAME + "-shared.log")), Level.INFO));
        appenders.add(createSharedLevelFileAppender("-warnings", Level.WARN));
        appenders.add(createSharedLevelFileAppender("-errors", Level.ERROR));

        if (logTestRunsToConsole)
        {
            ConsoleAppender<ILoggingEvent> consoleAppender = new ConsoleAppender<>();
            consoleAppender.setContext(loggerContext);
            consoleAppender.setEncoder(patternLayoutEncoder);
            consoleAppender.start();
            appenders.add(consoleAppender);
        }

        return createLogger(SHARED_LOGGER_NAME, appenders);
    }

    private Logger createLogger(String name, List<OutputStreamAppender<ILoggingEvent>> appenders)
    {
        Logger logger = loggerContext.getLogger(name);

        appenders.forEach(logger::addAppender);

        return logger;
    }
}
