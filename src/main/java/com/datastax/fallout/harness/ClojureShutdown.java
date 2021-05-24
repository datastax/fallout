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
package com.datastax.fallout.harness;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

import clojure.lang.IFn;

import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.ScopedLogger;

/** Manages shutting down clojure thread pool to prevent JVM shutdown delay
 *  <p> See https://puredanger.github.io/tech.puredanger.com/2010/06/08/clojure-agent-thread-pools;
 *  TL;DR: clojure creates non-daemon threads in its send-off pool, which need to
 *  be harvested manually if we don't want a 1 minute delay before exiting the JVM.
 *
 *  <p> Note that this shuts down clojure <em>in the current JVM</em> i.e. once you've called this, you
 *  cannot call anything in clojure again after this point.
 *
 *  <p> Not part of {@link ClojureApi} to prevent loading clojure when calling any of the
 *  methods; note that you <em>must not</em> import <code>clojure.java.api.Clojure</code>
 *  in this class <em>or any of its imports</em> for this code to work.
 */
public class ClojureShutdown
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(ClojureShutdown.class);

    private static boolean shutdownEnabled = true;

    public static void disableShutdown()
    {
        shutdownEnabled = false;
    }

    public static void enableShutdown()
    {
        shutdownEnabled = true;
    }

    /** Shutdown clojure only if enabled and if clojure has been loaded */
    public static void shutdown()
    {
        if (!shutdownEnabled)
        {
            return;
        }

        try
        {
            logger.withScopedInfo("Shutting down clojure if it was loaded").run(() -> {
                clojureLoaded().ifPresentOrElse(
                    clojureClass -> Exceptions.runUnchecked(() -> {
                        logger.info("Clojure was loaded; shutting down");
                        final var clojureVar = clojureClass
                            .getDeclaredMethod("var", Object.class, Object.class);
                        final var shutdownAgents =
                            (IFn) clojureVar.invoke(null, "clojure.core", "shutdown-agents");
                        shutdownAgents.invoke();
                    }),
                    () -> logger.info("Clojure was not loaded"));
            });
        }
        catch (Throwable e)
        {
            logger.error("Could not shut down clojure; the JVM may take up to a minute to exit", e);
        }
    }

    /** Do horrible classloader things to find out if Clojure has been loaded, so we can avoid accidentally loading
     *  it to shutdown (Clojure initialisation is static and <em>slow</em>, and thus we'd like to avoid it).
     * @return
     */
    private static Optional<Class<?>> clojureLoaded()
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
    {
        // Thank you https://stackoverflow.com/a/482909/322152
        final var findLoadedClass =
            ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
        findLoadedClass.setAccessible(true);

        final var classLoader = ClojureShutdown.class.getClassLoader();
        return Optional.ofNullable((Class<?>) findLoadedClass.invoke(classLoader, "clojure.java.api.Clojure"));
    }
}
