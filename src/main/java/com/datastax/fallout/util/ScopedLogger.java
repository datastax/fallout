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

import java.util.Optional;
import java.util.function.BooleanSupplier;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This is intended for use with try-with-resources blocks: it logs the specified message on entry
 *  and exit to the block.  In addition, those messages and any messages emitted by the info and error
 *  methods will be indented until the end of the block.  The indentation accumulates---a {@link ScopedLogger.Scoped}
 *  nested within the lifetime of another increases the indentation---and is thread-specific. */
public class ScopedLogger
{
    private final Logger logger;
    private static final ThreadLocal<Integer> indent = ThreadLocal.withInitial(() -> 0);
    private static final int INDENT_INCREMENT = 2;

    private interface LogMethod
    {
        void log(Logger logger, String msg, Object... args);
    }

    private interface LogEnabled extends BooleanSupplier
    {
    }

    private ScopedLogger(Logger logger)
    {
        this.logger = logger;
    }

    public static ScopedLogger getLogger(Logger logger)
    {
        return new ScopedLogger(logger);
    }

    public static ScopedLogger getLogger(Class<?> clazz)
    {
        return new ScopedLogger(LoggerFactory.getLogger(clazz));
    }

    public static ScopedLogger getLogger(String name)
    {
        return new ScopedLogger(LoggerFactory.getLogger(name));
    }

    public Logger delegate()
    {
        return logger;
    }

    private enum ResultExpectation
    {
        WITH_RESULT, WITHOUT_RESULT
    }

    private <T> Optional<Object> loggableResult(T result)
    {
        return Optional.of(result == null ? "<null>" : result);
    }

    private static String indent()
    {
        return Strings.repeat(" ", indent.get());
    }

    public class Scoped implements AutoCloseable
    {
        private final LogMethod logMethod;
        private final LogEnabled logEnabled;
        private final String msg;
        private final Object[] args;
        private Optional<Object> result = Optional.empty();
        private ResultExpectation resultExpectation;

        private Scoped(LogMethod logMethod, LogEnabled logEnabled, String msg, Object[] args)
        {
            this.logMethod = logMethod;
            this.logEnabled = logEnabled;
            this.resultExpectation = ResultExpectation.WITHOUT_RESULT;
            this.msg = msg;
            this.args = args;
            if (logEnabled.getAsBoolean())
            {
                logMethod.log(logger, indent() + msg + "...", args);
                indent.set(indent.get() + INDENT_INCREMENT);
            }
        }

        private <T> T completedNormallyWithResult(T result)
        {
            this.result = loggableResult(result);
            return result;
        }

        private void completedNormally()
        {
            this.result = Optional.of(true);
        }

        @Override
        public void close()
        {
            if (logEnabled.getAsBoolean())
            {
                indent.set(indent.get() - INDENT_INCREMENT);
                final var doneMsg = indent() + this.msg + "...done" + (result.isEmpty() ? " (exception thrown)" : "");
                final var resultMsg = result
                    .filter(ignored -> resultExpectation == ResultExpectation.WITH_RESULT)
                    .map(r -> " -> [" + r + "]")
                    .orElse("");
                logMethod.log(logger, doneMsg + resultMsg, args);
            }
        }

        public void expectResult()
        {
            resultExpectation = ResultExpectation.WITH_RESULT;
        }
    }

    // Public API

    public interface ThrowingSupplier<T, E extends Throwable>
    {
        T get() throws E;
    }

    public interface ThrowingRunnable<E extends Throwable>
    {
        void run() throws E;
    }

    public static class ScopedInvocation
    {
        private final Scoped scoped;

        private ScopedInvocation(Scoped scoped)
        {
            this.scoped = scoped;
        }

        public <E extends Throwable> void run(ThrowingRunnable<E> runnable) throws E
        {
            try (var scoped_ = scoped)
            {
                runnable.run();
                scoped.completedNormally();
            }
        }

        public <T, E extends Throwable> T get(ThrowingSupplier<T, E> supplier) throws E
        {
            try (var scoped_ = scoped)
            {
                scoped.expectResult();
                return scoped.completedNormallyWithResult(supplier.get());
            }
        }
    }

    public ScopedInvocation withScopedInfo(String msg, Object... args)
    {
        return new ScopedInvocation(new Scoped(
            (logger_, msg_, args_) -> logger.info(msg_, args_),
            logger::isInfoEnabled, msg, args));
    }

    public ScopedInvocation withScopedDebug(String msg, Object... args)
    {
        return new ScopedInvocation(new Scoped(
            (logger_, msg_, args_) -> logger.debug(msg_, args_),
            logger::isDebugEnabled, msg, args));
    }

    // Non-scoped logging methods that log the result of a call

    public class ResultInvocation
    {
        private final LogMethod logMethod;
        private final LogEnabled logEnabled;
        private final String msg;
        private final Object[] args;

        private ResultInvocation(LogMethod logMethod, LogEnabled logEnabled, String msg, Object... args)
        {
            this.logMethod = logMethod;
            this.logEnabled = logEnabled;
            this.msg = msg;
            this.args = args;
        }

        public <T, E extends Throwable> T get(ThrowingSupplier<T, E> supplier) throws E
        {
            Optional<Object> completedWithResult = Optional.empty();

            try
            {
                final var result = supplier.get();
                completedWithResult = loggableResult(result);
                return result;
            }
            finally
            {
                if (logEnabled.getAsBoolean())
                {
                    logMethod.log(logger, indent() + msg + " -> " + completedWithResult
                        .map(r -> "[" + r + "]")
                        .orElse("<exception thrown>"), args);
                }
            }
        }
    }

    public ResultInvocation withResultInfo(String msg, Object... args)
    {
        return new ResultInvocation(Logger::info, logger::isInfoEnabled, msg, args);
    }

    public ResultInvocation withResultDebug(String msg, Object... args)
    {
        return new ResultInvocation(Logger::debug, logger::isDebugEnabled, msg, args);
    }

    // Logger API methods, so we can just switch to using ScopedLogger

    public void trace(String msg, Object... args)
    {
        if (logger.isTraceEnabled())
        {
            logger.trace(indent() + msg, args);
        }
    }

    public void debug(String msg, Object... args)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(indent() + msg, args);
        }
    }

    public void info(String msg, Object... args)
    {
        if (logger.isInfoEnabled())
        {
            logger.info(indent() + msg, args);
        }
    }

    public void warn(String msg, Object... args)
    {
        if (logger.isWarnEnabled())
        {
            logger.warn(indent() + msg, args);
        }
    }

    public void error(String msg, Object... args)
    {
        if (logger.isErrorEnabled())
        {
            logger.error(indent() + msg, args);
        }
    }

    public void trace(String msg, Throwable t)
    {
        if (logger.isTraceEnabled())
        {
            logger.trace(indent() + msg, t);
        }
    }

    public void debug(String msg, Throwable t)
    {
        if (logger.isDebugEnabled())
        {
            logger.debug(indent() + msg, t);
        }
    }

    public void info(String msg, Throwable t)
    {
        if (logger.isInfoEnabled())
        {
            logger.info(indent() + msg, t);
        }
    }

    public void warn(String msg, Throwable t)
    {
        if (logger.isWarnEnabled())
        {
            logger.warn(indent() + msg, t);
        }
    }

    public void error(String msg, Throwable t)
    {
        if (logger.isErrorEnabled())
        {
            logger.error(indent() + msg, t);
        }
    }
}
