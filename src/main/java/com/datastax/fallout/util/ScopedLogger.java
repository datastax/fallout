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
package com.datastax.fallout.util;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

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

    public class Scoped implements AutoCloseable
    {
        private final LogMethod logMethod;
        private final LogEnabled logEnabled;
        private final String msg;
        private final Object[] args;
        private Optional<Object> result = Optional.empty();

        private Scoped(LogMethod logMethod, LogEnabled logEnabled, String msg, Object[] args)
        {
            this.logMethod = logMethod;
            this.logEnabled = logEnabled;
            this.msg = msg;
            this.args = args;
            if (logEnabled.getAsBoolean())
            {
                logMethod.log(logger, indent() + msg + "...", args);
                indent.set(indent.get() + INDENT_INCREMENT);
            }
        }

        public <T> T setResult(T result)
        {
            this.result = loggableResult(result);
            return result;
        }

        @Override
        public void close()
        {
            if (logEnabled.getAsBoolean())
            {
                indent.set(indent.get() - INDENT_INCREMENT);
                result(logMethod, result, msg + "...done", args);
            }
        }
    }

    private <T> Optional<Object> loggableResult(T result)
    {
        return Optional.of(result == null ? "<null>" : result);
    }

    private void result(LogMethod logMethod, Optional<Object> result, String msg, Object... args)
    {
        logMethod.log(logger, msg + result.map(r -> " -> [" + r + "]").orElse(""), args);
    }

    private <T> T result(LogMethod logMethod, LogEnabled logEnabled, Supplier<T> supplier, String msg, Object... args)
    {
        final var result = supplier.get();
        if (logEnabled.getAsBoolean())
        {
            result(logMethod, loggableResult(result), msg, args);
        }
        return result;
    }

    public <T> T resultDebug(Supplier<T> supplier, String msg, Object... args)
    {
        return result(Logger::debug, logger::isDebugEnabled, supplier, msg, args);
    }

    public <T> T doWithScopedInfo(Supplier<T> supplier, String msg, Object... args)
    {
        try (Scoped scoped = scopedInfo(msg, args))
        {
            return scoped.setResult(supplier.get());
        }
    }

    public void doWithScopedInfo(Runnable runnable, String msg, Object... args)
    {
        try (Scoped ignored = scopedInfo(msg, args))
        {
            runnable.run();
        }
    }

    public Scoped scopedInfo(String msg, Object... args)
    {
        return new Scoped(
            (logger_, msg_, args_) -> logger.info(msg_, args_),
            logger::isInfoEnabled, msg, args);
    }

    public <T> T doWithScopedDebug(Supplier<T> supplier, String msg, Object... args)
    {
        try (Scoped scoped = scopedDebug(msg, args))
        {
            return scoped.setResult(supplier.get());
        }
    }

    public void doWithScopedDebug(Runnable runnable, String msg, Object... args)
    {
        try (Scoped ignored = scopedDebug(msg, args))
        {
            runnable.run();
        }
    }

    public Scoped scopedDebug(String msg, Object... args)
    {
        return new Scoped(
            (logger_, msg_, args_) -> logger.debug(msg_, args_),
            logger::isDebugEnabled, msg, args);
    }

    private static String indent()
    {
        return Strings.repeat(" ", indent.get());
    }

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
