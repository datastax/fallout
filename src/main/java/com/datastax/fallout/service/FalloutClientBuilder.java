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

import javax.ws.rs.client.Client;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.dropwizard.jersey.jackson.JacksonFeature;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.logging.LoggingFeature;

import com.datastax.fallout.util.JacksonUtils;
import com.datastax.fallout.util.NamedThreadFactory;

public class FalloutClientBuilder
{
    private final JerseyClientBuilder jerseyClientBuilder;

    private FalloutClientBuilder(JerseyClientBuilder jerseyClientBuilder)
    {
        this.jerseyClientBuilder = jerseyClientBuilder;
    }

    private static String loggerName(String name)
    {
        return FalloutClientBuilder.class.getName() + "." + name + "-client";
    }

    public static FalloutClientBuilder named(String name)
    {
        return new FalloutClientBuilder(createDefaultClientBuilder(loggerName(name), name));
    }

    public static FalloutClientBuilder forEnvironment(Environment environment)
    {
        return new FalloutClientBuilder(createDefaultClientBuilder(
            loggerName(environment.getName()), environment.getName()));
    }

    public static FalloutClientBuilder forComponent(Class<?> componentClass)
    {
        return new FalloutClientBuilder(createDefaultClientBuilder(
            componentClass.getName() + "." + "client", componentClass.getSimpleName()));
    }

    private static JerseyClientBuilder createDefaultClientBuilder(String loggerName, String threadName)
    {
        /* Note that this is a {@link java.util.logging.Logger}; Dropwizard automatically
         * installs the {@link org.slf4j.bridge.SLF4JBridgeHandler} to redirect all j.u.l.
         * logging to logback.  We just need to initialise this logger with a non-restrictive
         * log-level, then all the Jersey client logging will be controllable by
         * Dropwizard.   Note that all the _useful_ logging only appears at TRACE level. */
        final Logger logger = Logger.getLogger(loggerName);
        logger.setLevel(Level.ALL);

        final var builder = new JerseyClientBuilder()
            // Prevent accidental inclusion of dependencies modifying client behaviour
            .property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
            .property(ClientProperties.CONNECT_TIMEOUT, (int) Duration.ofMinutes(1).toMillis())
            .property(ClientProperties.READ_TIMEOUT, (int) Duration.ofMinutes(1).toMillis())
            .register(new JacksonFeature(JacksonUtils.getObjectMapper()))
            .register(new LoggingFeature(logger, Level.ALL, LoggingFeature.Verbosity.PAYLOAD_ANY, null));

        // Provide our own ExecutorServices to this instance so that a) we can name the threads and b) we can
        // ensure they're daemon threads (so that anything that fails to close a client won't hold up graceful
        // shutdown; the default jersey executors use non-daemon threads).
        final var executorService = Executors.newCachedThreadPool(
            new NamedThreadFactory(threadName + "-client-async"));
        final var scheduledExecutorService = Executors.newScheduledThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory(threadName + "-client-background"));

        builder.executorService(executorService);
        builder.scheduledExecutorService(scheduledExecutorService);

        return builder;
    }

    public Client build()
    {
        return jerseyClientBuilder.build();
    }
}
