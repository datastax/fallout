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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.assertj.Assertions.assertThatCode;

public class ScopedLoggerTest
{
    private LoggerContext loggerContext;
    private Logger logger;
    private ScopedLogger scopedLogger;
    private ConcurrentHashMap<String, List<ILoggingEvent>> events;
    private ThreadSpecificAppender appender;

    private class ThreadSpecificAppender extends AppenderBase<ILoggingEvent>
    {
        @Override
        protected void append(ILoggingEvent eventObject)
        {
            // Note that calling getThreadName() actually _populates_ the thread name; if we don't call it,
            // it will be populated at the call site.
            events.computeIfAbsent(eventObject.getThreadName(), ignored -> new ArrayList<>()).add(eventObject);
        }
    }

    @BeforeEach
    public void setUp()
    {
        loggerContext = new LoggerContext();
        loggerContext.start();
        logger = loggerContext.getLogger(ScopedLoggerTest.class);
        logger.detachAndStopAllAppenders();

        events = new ConcurrentHashMap<>();

        appender = new ThreadSpecificAppender();
        appender.setContext(loggerContext);
        appender.start();
        logger.addAppender(appender);

        scopedLogger = ScopedLogger.getLogger(logger);
    }

    @AfterEach
    public void tearDown()
    {
        loggerContext.stop();
    }

    private List<Pair<Level, String>> logMessagesByLevel()
    {
        return events.values().stream().flatMap(Collection::stream)
            .map(event -> Pair.of(event.getLevel(), event.getFormattedMessage()))
            .collect(Collectors.toList());
    }

    private Map<String, List<String>> logMessagesByThread()
    {
        return events.entrySet().stream()
            .map(entry -> Pair.of(entry.getKey(), entry.getValue().stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.toList())))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    @Test
    public void exceptions_are_reported()
    {
        assertThatCode(() -> {
            scopedLogger.withScopedInfo("a thing").run(() -> {
                scopedLogger.debug("stuff happens");
                throw new RuntimeException("bang");
            });
        })
            .isInstanceOf(RuntimeException.class);

        assertThat(logMessagesByLevel()).containsExactly(
            Pair.of(Level.INFO, "a thing..."),
            Pair.of(Level.DEBUG, "  stuff happens"),
            Pair.of(Level.INFO, "a thing...done (exception thrown)"));
    }

    @Test
    public void inner_log_messages_are_indented()
    {
        scopedLogger.withScopedInfo("a thing").run(() -> {
            scopedLogger.debug("stuff happens");
            scopedLogger.info("info stuff happens");
            scopedLogger.withScopedDebug("a debugging thing").run(() -> {
                scopedLogger.error("oh noes");
            });
        });

        assertThat(logMessagesByLevel()).containsExactly(
            Pair.of(Level.INFO, "a thing..."),
            Pair.of(Level.DEBUG, "  stuff happens"),
            Pair.of(Level.INFO, "  info stuff happens"),
            Pair.of(Level.DEBUG, "  a debugging thing..."),
            Pair.of(Level.ERROR, "    oh noes"),
            Pair.of(Level.DEBUG, "  a debugging thing...done"),
            Pair.of(Level.INFO, "a thing...done"));
    }

    @Test
    public void indentation_is_per_thread()
    {
        final var outerThreadName = Thread.currentThread().getName();

        scopedLogger.info("thread 1 outer");

        final var innerThreadName = scopedLogger.withScopedInfo("thread 1 scoped").get(() -> {
            scopedLogger.info("thread 1 inner");
            return CompletableFuture.supplyAsync(() -> {
                scopedLogger.info("thread 2 outer");
                scopedLogger.withScopedInfo("thread 2 scoped").run(() -> {
                    scopedLogger.info("thread 2 inner");
                });
                return Thread.currentThread().getName();
            }).join();
        });

        assertThat(logMessagesByThread()).containsExactlyInAnyOrderEntriesOf(Map.of(
            outerThreadName, List.of(
                "thread 1 outer",
                "thread 1 scoped...",
                "  thread 1 inner",
                String.format("thread 1 scoped...done -> [%s]", innerThreadName)),
            innerThreadName, List.of(
                "thread 2 outer",
                "thread 2 scoped...",
                "  thread 2 inner",
                "thread 2 scoped...done")));
    }

    @Test
    public void results_are_shown()
    {
        assertThat(scopedLogger.withScopedInfo("getting the magic number").get(() -> 3)).isEqualTo(3);

        assertThat(logMessagesByLevel()).containsExactly(
            Pair.of(Level.INFO, "getting the magic number..."),
            Pair.of(Level.INFO, "getting the magic number...done -> [3]"));
    }

    @Test
    public void results_are_not_shown_when_exceptions_are_thrown()
    {
        assertThatCode(() -> scopedLogger.withScopedInfo("getting the magic number").get(() -> {
            throw new RuntimeException();
        }))
            .isInstanceOf(RuntimeException.class);

        assertThat(logMessagesByLevel()).containsExactly(
            Pair.of(Level.INFO, "getting the magic number..."),
            Pair.of(Level.INFO, "getting the magic number...done (exception thrown)"));
    }

    @Test
    public void unscoped_results_are_shown()
    {
        assertThat(scopedLogger.withResultDebug("getting the magic number").get(() -> 3)).isEqualTo(3);

        assertThat(logMessagesByLevel()).containsExactly(
            Pair.of(Level.DEBUG, "getting the magic number -> [3]"));
    }

    @Test
    public void unscoped_results_are_not_shown_when_exceptions_are_thrown()
    {
        assertThatCode(() -> scopedLogger.withResultDebug("getting the magic number").get(() -> {
            throw new RuntimeException();
        }))
            .isInstanceOf(RuntimeException.class);

        assertThat(logMessagesByLevel()).containsExactly(
            Pair.of(Level.DEBUG, "getting the magic number -> <exception thrown>"));
    }

}
