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
package com.datastax.fallout.service.resources;

import java.net.URI;
import java.net.URL;
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
import io.dropwizard.testing.junit5.DropwizardAppExtension;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.fallout.LogbackConfigurator;
import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.FalloutServiceBase;
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
import com.datastax.fallout.util.component_discovery.MockingComponentFactory;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Unlike {@link DropwizardAppExtension}, this is designed to be used directly using
 *  {@link org.junit.jupiter.api.extension.RegisterExtension}, as either a static or
 *  non-static field.  This is handled correctly by maintaining an {@link #activeScopes} count
 *  and using that to ensure we only execute during the appropriate before/after scope.
 *
 *  <p>{@link DropwizardAppExtension} relies on {@link io.dropwizard.testing.junit5.DropwizardExtensionsSupport}
 *  being used as a class-level {@link org.junit.jupiter.api.extension.ExtendWith}; however:
 *
 *  <ul>
 *      <li>this drops all {@link ExtensionContext} parameters (and we want those in this class);
 *
 *      <li>(less importantly) it makes any field deriving from {@link io.dropwizard.testing.junit5.DropwizardExtension}
 *      "magic": such fields no longer require an annotation to make them have the before/after behaviour;
 *
 *      <li>since {@link FalloutAppExtensionBase.FalloutServiceResetExtension} still
 *      requires a {@link org.junit.jupiter.api.extension.RegisterExtension} annotation,
 *      it makes the {@link FalloutAppExtensionBase} field declaration asymmetric.
 *  </ul>
 */
public class FalloutAppExtensionBase<FC extends FalloutConfiguration, FS extends FalloutServiceBase<FC>>
    extends DropwizardAppExtension<FC>
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback
{
    private Path falloutHome;
    private Path artifactPath;
    private CompletableFuture<Integer> serverPort = new CompletableFuture<>();
    private Optional<FalloutAppExtensionBase<FC, FS>> runnerServiceExtension = Optional.empty();
    private boolean isSchemaCreator = true;
    private int activeScopes = 0;

    public static final String CASSANDRA_KEYSPACE = "test";

    private static class TestSupport<FC extends FalloutConfiguration, FS extends FalloutServiceBase<FC>>
        extends DropwizardTestSupport<FC>
    {
        private MockingComponentFactory componentFactory;

        private final TestRunStatusUpdatePublisher runnerTestRunStatusFeed = new TestRunStatusUpdatePublisher();

        private static ConfigOverride[] addToConfigOverrides(ConfigOverride[] configOverrides)
        {
            return Stream
                .concat(Stream.of(
                    ConfigOverride.config("keyspace", CASSANDRA_KEYSPACE),
                    ConfigOverride.config("logTestRunsToConsole", "true")),
                    Stream.concat(
                        Stream.of(configOverrides),
                        LogbackConfigurator.getConfigOverrides()))
                .toArray(ConfigOverride[]::new);
        }

        TestSupport(Class<? extends FalloutServiceBase<FC>> falloutServiceClass, String configPath,
            FalloutConfiguration.ServerMode mode, ConfigOverride... configOverrides)
        {
            super(falloutServiceClass, configPath, (String) null,
                application -> {
                    FalloutServerCommand<FC> command = null;
                    switch (mode)
                    {
                        case STANDALONE:
                            command = new FalloutStandaloneCommand<>((FalloutServiceBase<FC>) application) {
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
                            command = new FalloutQueueCommand<>((FalloutServiceBase<FC>) application) {
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
                            command = new FalloutRunnerCommand<>((FalloutServiceBase<FC>) application) {
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

        @SuppressWarnings("unchecked")
        @Override
        public FS newApplication()
        {
            FS falloutService = (FS) super.newApplication();
            componentFactory = new MockingComponentFactory(falloutService.getComponentFactory());
            falloutService.setComponentFactory(componentFactory);
            falloutService.withRunnerTestRunStatusFeed(runnerTestRunStatusFeed);
            return falloutService;
        }
    }

    private void ensureEmptyDirectoryExists(Path path)
    {
        FileUtils.createDirs(path);
        assertThat(FileUtils.listDir(path).isEmpty());
    }

    public FalloutAppExtensionBase(Class<FS> falloutServiceClass,
        FalloutConfiguration.ServerMode mode, String configPath,
        ConfigOverride... configOverrides)
    {
        super(new TestSupport<>(falloutServiceClass, configPath, mode, configOverrides));

        // ServiceListener.onRun will run _after_ configuration setup and _before_ starting the server; this is ideal
        // for setting up things that need the configuration but need to be in place before startup.
        addListener(new ServiceListener<>() {
            @Override
            public void onRun(FC configuration, Environment environment,
                DropwizardAppExtension<FC> rule)
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

                environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
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
            final FalloutAppExtensionBase<FC, FS> runnerServiceExtension =
                new FalloutAppExtensionBase<>(falloutServiceClass,
                    FalloutConfiguration.ServerMode.RUNNER, configPath, configOverrides);

            // The RUNNER FalloutServiceRule will create the schema for us, because RUNNER is created first, so the
            // QUEUE should run in its default mode.
            isSchemaCreator = false;
            this.runnerServiceExtension = Optional.of(runnerServiceExtension);
        }
    }

    private void setTestOutputDir(Path testOutputDir)
    {
        falloutHome = testOutputDir;
        artifactPath = TestHelpers.setupArtifactPath(testOutputDir);
        runnerServiceExtension.ifPresent(rule -> rule.setTestOutputDir(testOutputDir));
    }

    public Path getArtifactPath()
    {
        return artifactPath;
    }

    public MockingComponentFactory componentFactory()
    {
        return ((TestSupport<FC, FS>) getTestSupport()).componentFactory;
    }

    public TestRunStatusUpdatePublisher runnerTestRunStatusFeed()
    {
        return ((TestSupport<FC, FS>) getTestSupport()).runnerTestRunStatusFeed;
    }

    @SuppressWarnings("unchecked")
    @Override
    public FS getApplication()
    {
        return super.getApplication();
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

    public void before(Path persistentTestOutputDir) throws Exception
    {
        setTestOutputDir(persistentTestOutputDir);

        // Start the RUNNER if required, so that the QUEUE finds it on startup
        runnerServiceExtension.ifPresent(rule -> Exceptions.runUnchecked(rule::before));

        // Start the server
        super.before();

        waitForServerStartup();
    }

    @Override
    public void after()
    {
        runnerServiceExtension.ifPresent(FalloutAppExtensionBase::after);

        getApplication().shutdown();
        super.after();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception
    {
        if (activeScopes++ == 0)
        {
            assertThat(context.getTestClass()).isNotEmpty();
            before(WithPersistentTestOutputDir
                .persistentTestOutputDir(context.getTestClass().get(),
                    context.getTestMethod().map(testMethod -> Pair.of(testMethod, context.getDisplayName()))));
        }
    }

    @Override
    public void afterAll(ExtensionContext context)
    {
        if (--activeScopes == 0)
        {
            after();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception
    {
        beforeAll(context);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception
    {
        afterAll(context);
    }

    public enum ResetBehavior
    {
        CLEAN_DATABASE,
        DO_NOT_CLEAN_DATABASE
    }

    public class FalloutServiceResetExtension implements BeforeEachCallback
    {
        private FalloutClient falloutClient;
        private final ResetBehavior resetBehavior;

        public FalloutServiceResetExtension(
            ResetBehavior resetBehavior)
        {
            this.resetBehavior = resetBehavior;
        }

        public void before()
        {
            await()
                .atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> assertThat(getApplication().runningTestRunsCount()).isEqualTo(0));

            if (resetBehavior == ResetBehavior.CLEAN_DATABASE)
            {
                maybeCleanDatabase();
            }

            falloutClient = new FalloutClient(getFalloutServiceUri(), FalloutAppExtensionBase.this::getSession);
        }

        @Override
        public void beforeEach(ExtensionContext context) throws Exception
        {
            before();
        }

        public RestApiBuilder anonApi()
        {
            return falloutClient.anonApi();
        }

        public RestApiBuilder userApi()
        {
            return falloutClient.userApi();
        }

        public RestApiBuilder corruptUserApi()
        {
            return falloutClient.corruptUserApi();
        }

        public RestApiBuilder adminApi()
        {
            return falloutClient.adminApi();
        }
    }

    /** Returns an external resource for use with the {@link
     *  org.junit.jupiter.api.extension.RegisterExtension} annotation */
    public FalloutServiceResetExtension resetExtension(ResetBehavior resetBehavior)
    {
        return new FalloutServiceResetExtension(resetBehavior);
    }

    public FalloutServiceResetExtension resetExtension()
    {
        return resetExtension(ResetBehavior.CLEAN_DATABASE);
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
