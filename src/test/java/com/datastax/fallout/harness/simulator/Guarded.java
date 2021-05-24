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
package com.datastax.fallout.harness.simulator;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protects an instance of T from multi-threaded access.
 *
 * The main reason for this class' existence is to abstract the boilerplate of basic java monitors
 * into a single method, {@link Guarded.Guard#apply}.  The remaining methods are used to build
 * up the information used by the latter: setting up a wait predicate and description ({@link
 * Guarded.Guard#waitUntil}, {@link Guarded.Guard#withPredicateDescription}); whether timeouts should be
 * used and how they should be handled ({@link Guarded.Guard#withTimeout}); and what we should do after
 * a wait, and whether we should notify waiters ({@link Guarded#accept}, {@link Guarded#notifyAfter}).
 */
public class Guarded<T>
{
    private static final Logger logger = LoggerFactory.getLogger(Guarded.class);

    private final Lock lock = new ReentrantLock();
    private final Condition cond = lock.newCondition();
    private final T guarded;
    private final String name;
    private final Function<T, String> formatter;

    Guarded(T guarded, String name)
    {
        this(guarded, name, String::valueOf);
    }

    Guarded(T guarded, String name, Function<T, String> formatter)
    {
        this.guarded = guarded;
        this.name = name;
        this.formatter = formatter;
    }

    private void debug(String message, Object... args)
    {
        logger.info(name + ": " + message, args);
    }

    private boolean debugEnabled()
    {
        return logger.isInfoEnabled();
    }

    class Guard
    {
        private Optional<Function<T, Boolean>> waitUntilPredicate = Optional.empty();
        private String predicateDescription = "<unknown predicate>";
        private String operationDescription = "<unknown operation>";
        private boolean doNotify = false;
        private Consumer<String> timeoutAssertionHandler = name -> {};
        private Optional<Duration> timeout = Optional.empty();

        Guard waitUntil(Function<T, Boolean> waitUntilPredicate)
        {
            this.waitUntilPredicate = Optional.of(waitUntilPredicate);
            return this;
        }

        Guard withPredicateDescription(String predicateDescription)
        {
            this.predicateDescription = predicateDescription;
            return this;
        }

        Guard withOperationDescription(String operationDescription)
        {
            this.operationDescription = operationDescription;
            return this;
        }

        Guard withTimeout(Duration timeout, Consumer<String> timeoutAssertionHandler)
        {
            this.timeout = Optional.of(timeout);
            this.timeoutAssertionHandler = timeoutAssertionHandler;
            return this;
        }

        private boolean waitDidNotTimeout()
        {
            try
            {
                if (timeout.isPresent())
                {
                    return cond.await(timeout.get().toMillis(), TimeUnit.MILLISECONDS);
                }
                else
                {
                    cond.await();
                    return true;
                }
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        Guard notifyAfter()
        {
            doNotify = true;
            return this;
        }

        private void doWaitUntil()
        {
            waitUntilPredicate.ifPresent(predicate -> {
                int waits = 1;
                while (!predicate.apply(guarded))
                {
                    debug("Waiting ({} times) for {}: {}...", waits, formatter.apply(guarded),
                        predicateDescription);

                    if (!waitDidNotTimeout())
                    {
                        debug("Waiting ({} times) for {}: {}...timed out", waits, formatter.apply(guarded),
                            predicateDescription);
                        timeoutAssertionHandler.accept(name);
                    }

                    ++waits;
                }
                debug("Waiting ({} times) for {}: {}...done", waits, formatter.apply(guarded),
                    predicateDescription);
            });
        }

        private void doNotify()
        {
            if (doNotify)
            {
                cond.signalAll();
            }
        }

        private <U> U withLock(Supplier<U> resultSupplier)
        {
            lock.lock();
            try
            {
                return resultSupplier.get();
            }
            finally
            {
                lock.unlock();
            }
        }

        private void withLock(Runnable run)
        {
            withLock(() -> {
                run.run();
                return null;
            });
        }

        <U> U apply(Function<T, U> f)
        {
            return apply(f, String::valueOf);
        }

        <U> U apply(Function<T, U> f, Function<U, String> resultFormatter)
        {
            return withLock(() -> {
                doWaitUntil();

                Optional<String> previousValue = debugEnabled() ?
                    Optional.of(formatter.apply(guarded)) :
                    Optional.empty();

                U result = f.apply(guarded);

                previousValue.ifPresent(previousValue_ -> debug("Applied {} to {} -> {} = {}", operationDescription,
                    previousValue_, formatter.apply(guarded), resultFormatter.apply(result)));

                doNotify();

                return result;
            });
        }

        void accept(Consumer<T> f)
        {
            withLock(() -> {
                doWaitUntil();

                Optional<String> previousValue = debugEnabled() ?
                    Optional.of(formatter.apply(guarded)) :
                    Optional.empty();

                f.accept(guarded);

                previousValue.ifPresent(previousValue_ -> debug("Applied {} to {} -> {}", operationDescription,
                    previousValue_, formatter.apply(guarded)));

                doNotify();
            });
        }

        void noop()
        {
            withLock(() -> {
                doWaitUntil();
                doNotify();
            });
        }
    }

    Guard guard()
    {
        return new Guard();
    }

    Guard waitUntil(Function<T, Boolean> waitUntilPredicate)
    {
        return guard().waitUntil(waitUntilPredicate);
    }

    Guard notifyAfter()
    {
        return guard().notifyAfter();
    }

    void accept(Consumer<T> f)
    {
        guard().accept(f);
    }
}
