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
package com.datastax.fallout.service;

import javax.ws.rs.client.Client;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.jackson.JacksonFeature;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.logging.LoggingFeature;

public class FalloutClientBuilder
{
    private final JerseyClientBuilder jerseyClientBuilder;

    private static final ObjectMapper OBJECT_MAPPER = Jackson.newObjectMapper()
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

    public static ObjectMapper getObjectMapper()
    {
        return OBJECT_MAPPER;
    }

    private FalloutClientBuilder(String name)
    {
        jerseyClientBuilder = createDefaultClientBuilder(name);
    }

    private FalloutClientBuilder(Environment environment)
    {
        jerseyClientBuilder = createDefaultClientBuilder(environment.getName());
    }

    public static FalloutClientBuilder named(String name)
    {
        return new FalloutClientBuilder(name);
    }

    public static FalloutClientBuilder forEnvironment(Environment environment)
    {
        return new FalloutClientBuilder(environment);
    }

    public FalloutClientBuilder withExecutorService(ExecutorService executorService)
    {
        jerseyClientBuilder.executorService(executorService);
        return this;
    }

    private static JerseyClientBuilder createDefaultClientBuilder(String name)
    {
        /* Note that this is a {@link java.util.logging.Logger}; Dropwizard automatically
         * installs the {@link org.slf4j.bridge.SLF4JBridgeHandler} to redirect all j.u.l.
         * logging to logback.  We just need to initialise this logger with a non-restrictive
         * log-level, then all the Jersey client logging will be controllable by Dropwizard. */
        final Logger logger = Logger.getLogger(FalloutClientBuilder.class.getName() + "." + name + "-client");
        logger.setLevel(Level.ALL);

        return new JerseyClientBuilder()
            // Prevent accidental inclusion of dependencies modifying client behaviour
            .property(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE, true)
            .property(ClientProperties.CONNECT_TIMEOUT, (int) Duration.ofMinutes(1).toMillis())
            .property(ClientProperties.READ_TIMEOUT, (int) Duration.ofMinutes(1).toMillis())
            .register(new JacksonFeature(OBJECT_MAPPER))
            .register(new LoggingFeature(logger, Level.ALL, LoggingFeature.Verbosity.PAYLOAD_ANY, null));
    }

    public Client build()
    {
        return jerseyClientBuilder.build();
    }
}
