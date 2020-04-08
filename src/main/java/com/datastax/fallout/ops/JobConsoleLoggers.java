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

import java.nio.file.Path;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;

public class JobConsoleLoggers implements JobLoggers
{
    private LoggerContext loggerContext = new LoggerContext();

    private final org.slf4j.Logger shared = create("shared", null);

    @Override
    public org.slf4j.Logger create(String name, Path ignored)
    {
        Logger logger = loggerContext.getLogger(name);

        PatternLayoutEncoder layout = new PatternLayoutEncoder();
        layout.setContext(loggerContext);
        layout.setPattern(JobFileLoggers.FALLOUT_PATTERN);
        layout.start();

        ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
        appender.setContext(loggerContext);
        appender.setEncoder(layout);
        appender.start();

        logger.detachAndStopAllAppenders();
        logger.addAppender(appender);
        return logger;
    }

    @Override
    public org.slf4j.Logger getShared()
    {
        return shared;
    }
}
