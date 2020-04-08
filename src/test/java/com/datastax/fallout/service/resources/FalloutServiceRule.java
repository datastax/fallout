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
package com.datastax.fallout.service.resources;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.testing.ConfigOverride;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.junit.DropwizardAppRule;
import net.sourceforge.argparse4j.inf.Namespace;
import org.eclipse.jetty.server.Server;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.TestLogbackConfigurator;
import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.harness.TestRunnerTestHelpers;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutService;
import com.datastax.fallout.service.cli.FalloutQueueCommand;
import com.datastax.fallout.service.cli.FalloutRunnerCommand;
import com.datastax.fallout.service.cli.FalloutServerCommand;
import com.datastax.fallout.service.cli.FalloutStandaloneCommand;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.CassandraDriverManager.SchemaMode;
import com.datastax.fallout.service.db.CassandraDriverManagerHelpers;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.test.utils.WithPersistentTestOutputDir;
import com.datastax.fallout.util.Exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class FalloutServiceRule extends DropwizardAppRule<FalloutConfiguration>
{
    private Path falloutHome;
    private Path artifactPath;
    private CompletableFuture<Integer> serverPort = new CompletableFuture<>();
    private Optional<FalloutServiceRule> runnerServiceRule = Optional.empty();
    private boolean isSchemaCreator = true;

    public static final String CASSANDRA_KEYSPACE = "test";

    public FalloutServiceRule(ConfigOverride... configOverrides)
    {
        this(FalloutConfiguration.ServerMode.STANDALONE, configOverrides);
    }

    public FalloutServiceRule(FalloutConfiguration.ServerMode mode, ConfigOverride... configOverrides)
    {
        this(mode, null, configOverrides);
    }

    /** Used when we're not annotated with a @Rule */
    public static FalloutServiceRule withoutRuleAnnotation(FalloutConfiguration.ServerMode mode, Path testOutputDir,
        String configPath,
        ConfigOverride... configOverrides)
    {
        final FalloutServiceRule falloutServiceClassRule =
            new FalloutServiceRule(mode, configPath, configOverrides);
        falloutServiceClassRule.setTestOutputDir(testOutputDir);
        return falloutServiceClassRule;
    }

    private static class TestSupport extends DropwizardTestSupport<FalloutConfiguration>
    {
        private final TestRunnerTestHelpers.MockingComponentFactory componentFactory =
            new TestRunnerTestHelpers.MockingComponentFactory();

        private final TestRunStatusUpdatePublisher runnerTestRunStatusFeed = new TestRunStatusUpdatePublisher();

        private static ConfigOverride[] addToConfigOverrides(ConfigOverride[] configOverrides)
        {
            return Stream
                .concat(Stream.of(
                    ConfigOverride.config("keyspace", CASSANDRA_KEYSPACE),
                    ConfigOverride.config("logTestRunsToConsole", "true")),
                    Stream.concat(
                        Stream.of(configOverrides),
                        TestLogbackConfigurator.getConfigOverrides()))
                .toArray(ConfigOverride[]::new);
        }

        TestSupport(String configPath, FalloutConfiguration.ServerMode mode, ConfigOverride... configOverrides)
        {
            super(FalloutService.class, configPath, Optional.empty(),
                application -> {
                    FalloutServerCommand command = null;
                    switch (mode)
                    {
                        case STANDALONE:
                            command = new FalloutStandaloneCommand((FalloutService) application)
                            {
                                @Override
                                public void run(Bootstrap<?> wildcardBootstrap, Namespace namespace) throws Exception
                                {
                                    namespace = new Namespace(ImmutableMap.<String, Object>builder()
                                        .putAll(namespace.getAttrs())
                                        .put(FalloutServerCommand.PID_FILE_OPTION_NAME, false)
                                        .build());

                                    super.run(wildcardBootstrap, namespace);
                                }
                            };
                            break;
                        case QUEUE:
                            command = new FalloutQueueCommand((FalloutService) application)
                            {
                                @Override
                                public void run(Bootstrap<?> wildcardBootstrap, Namespace namespace) throws Exception
                                {
                                    namespace = new Namespace(ImmutableMap.<String, Object>builder()
                                        .putAll(namespace.getAttrs())
                                        .put(FalloutServerCommand.PID_FILE_OPTION_NAME, false)
                                        .put(FalloutQueueCommand.DELEGATE_RUNNER_ID_OPTION_NAME, 1)
                                        .build());

                                    super.run(wildcardBootstrap, namespace);
                                }
                            };
                            break;
                        case RUNNER:
                            command = new FalloutRunnerCommand((FalloutService) application)
                            {
                                @Override
                                public void run(Bootstrap<?> wildcardBootstrap, Namespace namespace) throws Exception
                                {
                                    namespace = new Namespace(ImmutableMap.<String, Object>builder()
                                        .putAll(namespace.getAttrs())
                                        .put(FalloutServerCommand.PID_FILE_OPTION_NAME, false)
                                        .put(FalloutRunnerCommand.PORT_FILE_OPTION_NAME, true)
                                        .put(FalloutRunnerCommand.RUNNER_ID_OPTION_NAME, 1)
                                        .build());

                                    super.run(wildcardBootstrap, namespace);
                                }
                            };
                            break;
                    }
                    return command;
                },
                addToConfigOverrides(configOverrides));
        }

        @Override
        public FalloutService newApplication()
        {
            FalloutService falloutService = (FalloutService) super.newApplication();
            falloutService.withComponentFactory(componentFactory);
            falloutService.withRunnerTestRunStatusFeed(runnerTestRunStatusFeed);
            return falloutService;
        }
    }

    private void ensureEmptyDirectoryExists(Path path)
    {
        Exceptions.runUncheckedIO(() -> {
            Files.createDirectories(path);
            assertThat(Files.list(path)).isEmpty();
        });
    }

    private FalloutServiceRule(FalloutConfiguration.ServerMode mode, String configPath,
        ConfigOverride... configOverrides)
    {
        super(new TestSupport(configPath, mode, configOverrides));

        // ServiceListener.onRun will run _after_ configuration setup and _before_ starting the server; this is ideal
        // for setting up things that need the configuration but need to be in place before startup.
        addListener(new ServiceListener<FalloutConfiguration>()
        {
            @Override
            public void onRun(FalloutConfiguration configuration, Environment environment,
                DropwizardAppRule<FalloutConfiguration> rule)
            {
                configuration.setFalloutHome(falloutHome);
                configuration.setArtifactPath(artifactPath.toString());

                // Don't create directories if this is the QUEUE process, otherwise we'd be racing
                // the subordinate RUNNER process.
                if (getConfiguration().getMode() != FalloutConfiguration.ServerMode.QUEUE)
                {
                    ensureEmptyDirectoryExists(artifactPath);
                    ensureEmptyDirectoryExists(configuration.getRunDir());
                }

                if (isSchemaCreator)
                {
                    getApplication().setPreCreateSchemaCallback(CassandraDriverManagerHelpers::dropSchema);
                }

                environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener()
                {
                    @Override
                    public void serverStarted(Server server)
                    {
                        serverPort.complete(getLocalPort(server));
                    }
                });
            }
        });

        if (mode == FalloutConfiguration.ServerMode.QUEUE)
        {
            final FalloutServiceRule runnerServiceRule = new FalloutServiceRule(
                FalloutConfiguration.ServerMode.RUNNER, configPath, configOverrides);

            // The RUNNER FalloutServiceRule will create the schema for us, because RUNNER is created first, so the
            // QUEUE should run in its default mode.
            isSchemaCreator = false;
            this.runnerServiceRule = Optional.of(runnerServiceRule);
        }
    }

    private void setTestOutputDir(Path testOutputDir)
    {
        falloutHome = testOutputDir;
        artifactPath = TestHelpers.setupArtifactPath(testOutputDir);
        runnerServiceRule.ifPresent(rule -> rule.setTestOutputDir(testOutputDir));
    }

    /** Called by the @Rule framework; if not using @Rule, use
     * {@link FalloutServiceRule#withoutRuleAnnotation} */
    @Override
    public Statement apply(Statement base, Description description)
    {
        setTestOutputDir(WithPersistentTestOutputDir.persistentTestOutputDir(description));
        return super.apply(base, description);
    }

    public Path getArtifactPath()
    {
        return artifactPath;
    }

    public TestRunnerTestHelpers.MockingComponentFactory componentFactory()
    {
        return ((TestSupport) getTestSupport()).componentFactory;
    }

    public TestRunStatusUpdatePublisher runnerTestRunStatusFeed()
    {
        return ((TestSupport) getTestSupport()).runnerTestRunStatusFeed;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FalloutService getApplication()
    {
        return (FalloutService) super.getApplication();
    }

    private CassandraDriverManager getCassandraDriverManager()
    {
        return getApplication().getCassandraDriverManager();
    }

    public CassandraDriverManager createCassandraDriverManager()
    {
        final var conf = getConfiguration();
        return new CassandraDriverManager(
            conf.getCassandraHost(), conf.getCassandraPort(), conf.getKeyspace(),
            SchemaMode.USE_EXISTING_SCHEMA, ignored -> {});
    }

    public Session getSession()
    {
        return getCassandraDriverManager().getSession();
    }

    private void waitForServerStartup()
    {
        serverPort.join();
    }

    @Override
    public void before() throws Exception
    {
        // Start the RUNNER if required, so that the QUEUE finds it on startup
        runnerServiceRule.ifPresent(rule -> Exceptions.runUnchecked(rule::before));

        // Start the server
        super.before();

        waitForServerStartup();
    }

    @Override
    public void after()
    {
        runnerServiceRule.ifPresent(FalloutServiceRule::after);

        getApplication().shutdown();
        super.after();
    }

    public enum ResetBehavior
    {
        CLEAN_DATABASE,
        DO_NOT_CLEAN_DATABASE
    }

    public class FalloutServiceResetRule extends ExternalResource
    {
        private FalloutClient falloutClient;
        private final ResetBehavior resetBehavior;

        public FalloutServiceResetRule(
            ResetBehavior resetBehavior)
        {
            this.resetBehavior = resetBehavior;
        }

        @Override
        public void before()
        {
            await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> assertThat(getApplication().runningTestRunsCount()).isEqualTo(0));

            if (resetBehavior == ResetBehavior.CLEAN_DATABASE)
            {
                maybeCleanDatabase();
            }

            falloutClient = new FalloutClient(getFalloutServiceUri(), FalloutServiceRule.this::getSession);
        }

        public RestApiBuilder anonApi()
        {
            return falloutClient.anonApi();
        }

        public RestApiBuilder userApi()
        {
            return falloutClient.userApi();
        }

        public RestApiBuilder adminApi()
        {
            return falloutClient.adminApi();
        }
    }

    /** Returns an external resource for use with the @Rule annotation */
    public FalloutServiceResetRule resetRule(ResetBehavior resetBehavior)
    {
        return new FalloutServiceResetRule(resetBehavior);
    }

    public FalloutServiceResetRule resetRule()
    {
        return resetRule(ResetBehavior.CLEAN_DATABASE);
    }

    private void maybeCleanDatabase()
    {
        if (getCassandraDriverManager() != null)
        {
            KeyspaceMetadata metadata = getSession().getCluster().getMetadata()
                .getKeyspace(CASSANDRA_KEYSPACE);
            metadata.getTables()
                .forEach(table -> {
                    getSession().execute(String.format("TRUNCATE %s.%s;",
                        CASSANDRA_KEYSPACE,
                        table.getName()));
                });
            TestRunDAO.addFinishedTestRunEndStop(CASSANDRA_KEYSPACE, getCassandraDriverManager().getMappingManager());
        }
    }

    private URI getFalloutServiceUri()
    {
        return Exceptions.getUnchecked(() -> new URL("http", "localhost", getLocalPort(), "/").toURI());
    }
}
