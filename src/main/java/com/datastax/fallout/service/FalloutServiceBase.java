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

import javax.servlet.DispatcherType;
import javax.servlet.Servlet;
import javax.servlet.ServletRegistration;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.Authorizer;
import io.dropwizard.auth.chained.ChainedAuthFilter;
import io.dropwizard.auth.oauth.OAuthCredentialAuthFilter;
import io.dropwizard.cli.CheckCommand;
import io.dropwizard.jersey.errors.IllegalStateExceptionMapper;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.lifecycle.AutoCloseableManager;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import io.dropwizard.views.freemarker.FreemarkerViewRenderer;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import io.netty.util.HashedWheelTimer;
import org.eclipse.jetty.rewrite.handler.RedirectRegexRule;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.glassfish.jersey.CommonProperties;
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature;
import org.glassfish.jersey.server.internal.LocalizationMessages;

import com.datastax.fallout.FalloutVersion;
import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.ClojureShutdown;
import com.datastax.fallout.harness.TestRunStatusUpdatePublisher;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.commands.CommandExecutor;
import com.datastax.fallout.ops.commands.LocalCommandExecutor;
import com.datastax.fallout.ops.utils.FileUtils;
import com.datastax.fallout.runner.AbortableRunnableExecutorFactory;
import com.datastax.fallout.runner.ActiveTestRunFactory;
import com.datastax.fallout.runner.DelegatingExecutorFactory;
import com.datastax.fallout.runner.DelegatingRunnableExecutorFactory;
import com.datastax.fallout.runner.DirectTestRunner;
import com.datastax.fallout.runner.JobLoggersFactory;
import com.datastax.fallout.runner.QueuingTestRunner;
import com.datastax.fallout.runner.ResourceReservationLocks;
import com.datastax.fallout.runner.RunnableExecutorFactory;
import com.datastax.fallout.runner.ThreadedRunnableExecutorFactory;
import com.datastax.fallout.runner.UserCredentialsFactory;
import com.datastax.fallout.runner.UserCredentialsFactory.UserCredentials;
import com.datastax.fallout.runner.queue.PersistentPendingQueue;
import com.datastax.fallout.service.FalloutConfiguration.ServerMode;
import com.datastax.fallout.service.artifacts.ArtifactCompressor;
import com.datastax.fallout.service.artifacts.ArtifactCompressorAdminTask;
import com.datastax.fallout.service.artifacts.ArtifactScrubber;
import com.datastax.fallout.service.artifacts.ArtifactUsageAdminTask;
import com.datastax.fallout.service.artifacts.ArtifactWatcher;
import com.datastax.fallout.service.artifacts.JettyArtifactServlet;
import com.datastax.fallout.service.artifacts.NginxArtifactServlet;
import com.datastax.fallout.service.auth.FalloutCookieAuthFilter;
import com.datastax.fallout.service.auth.FalloutTokenAuthenticator;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.auth.SingleUserAuthFilter;
import com.datastax.fallout.service.cli.Cassandra;
import com.datastax.fallout.service.cli.FalloutExecCommand;
import com.datastax.fallout.service.cli.FalloutQueueCommand;
import com.datastax.fallout.service.cli.FalloutRunnerCommand;
import com.datastax.fallout.service.cli.FalloutStandaloneCommand;
import com.datastax.fallout.service.cli.GenerateNginxConf;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;
import com.datastax.fallout.service.db.CassandraDriverManager;
import com.datastax.fallout.service.db.CassandraDriverManager.SchemaMode;
import com.datastax.fallout.service.db.PerformanceReportDAO;
import com.datastax.fallout.service.db.QueueMetricsManager;
import com.datastax.fallout.service.db.TestDAO;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.service.db.UserDAO;
import com.datastax.fallout.service.db.UserGroupMapper;
import com.datastax.fallout.service.resources.ServerSentEvents;
import com.datastax.fallout.service.resources.runner.RunnerResource;
import com.datastax.fallout.service.resources.server.AccountResource;
import com.datastax.fallout.service.resources.server.AdminResource;
import com.datastax.fallout.service.resources.server.ComponentResource;
import com.datastax.fallout.service.resources.server.HomeResource;
import com.datastax.fallout.service.resources.server.LiveResource;
import com.datastax.fallout.service.resources.server.PerformanceToolResource;
import com.datastax.fallout.service.resources.server.StatusResource;
import com.datastax.fallout.service.resources.server.TestResource;
import com.datastax.fallout.service.views.MainView;
import com.datastax.fallout.util.Duration;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FinishedTestRunUserNotifier;
import com.datastax.fallout.util.HtmlMailUserMessenger;
import com.datastax.fallout.util.JacksonUtils;
import com.datastax.fallout.util.MustacheViewRendererWithoutTemplatingErrors;
import com.datastax.fallout.util.NamedThreadFactory;
import com.datastax.fallout.util.ScopedLogger;
import com.datastax.fallout.util.SlackUserMessenger;
import com.datastax.fallout.util.UserMessenger;
import com.datastax.fallout.util.component_discovery.ComponentFactory;

public abstract class FalloutServiceBase<FC extends FalloutConfiguration> extends Application<FC>
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(FalloutServiceBase.class);

    public static final String COOKIE_NAME = "fallout-cookie";
    public static final String OAUTH_REALM = "fallout-realm";
    /**
     * https://tools.ietf.org/html/rfc6750
     */
    public static final String OAUTH_BEARER_TOKEN_TYPE = "Bearer";

    private ComponentFactory componentFactory;
    private Client httpClient;
    private CassandraDriverManager cassandraDriverManager;
    private IntSupplier runningTestRunsCount;
    private Consumer<CassandraDriverManager> preCreateSchemaCallback = ignored -> {};
    private Optional<TestRunStatusUpdatePublisher> runnerTestRunStatusFeed = Optional.empty();
    private Runnable shutdownHandler = () -> {};

    public static <FC extends FalloutConfiguration, FSB extends FalloutServiceBase<FC>>
        void main(Class<FSB> serviceClass, String[] args)
    {
        FSB service;
        try
        {
            service = serviceClass.getConstructor().newInstance();
            service.run(args);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }

    protected FalloutServiceBase(ComponentFactory componentFactory)
    {
        this.componentFactory = componentFactory;
    }

    @Override
    public String getName()
    {
        return "fallout";
    }

    /** Overridden to prevent adding any default commands */
    @Override
    protected void addDefaultCommands(Bootstrap<FC> bootstrap)
    {
    }

    private static final String ASSETS_ROOT_PATH = "/a";

    /** Sets up a rewrite rule to redirect requests for versioned assets to the standard asset
     *  root path, and returns a versioned asset path from which assets should be requested. */

    private static String addVersionedAssetsRewriteRule(RewriteHandler rewriteHandler)
    {
        rewriteHandler.addRule(new RewriteRegexRule("/assets\\.[^\\/]*/(.*)",
            ASSETS_ROOT_PATH + "/$1"));
        return "/assets." + FalloutVersion.getCommitHash().substring(0, 8);
    }

    @Override
    public void initialize(Bootstrap<FC> bootstrap)
    {
        bootstrap.addBundle(new ViewBundle<FC>(Set.of(
            new MustacheViewRendererWithoutTemplatingErrors(),
            // SwaggerBundle uses Freemarker templates
            new FreemarkerViewRenderer())) {
            @Override
            public Map<String, Map<String, String>> getViewConfiguration(FC configuration)
            {
                return Boolean.getBoolean("fallout.devmode") ?
                    Map.of("mustache", Map.of(
                        "cache", "false",
                        "fileRoot", "src/main/resources")) :
                    Map.of();
            }
        });

        bootstrap.addBundle(new AssetsBundle("/assets", ASSETS_ROOT_PATH));

        bootstrap.addCommand(new FalloutExecCommand<>(this));
        bootstrap.addCommand(new FalloutStandaloneCommand<>(this));
        bootstrap.addCommand(new FalloutRunnerCommand<>(this));
        bootstrap.addCommand(new FalloutQueueCommand<>(this));
        bootstrap.addCommand(new CheckCommand<>(this));
        bootstrap.addCommand(new GenerateNginxConf<>(this));
        bootstrap.addCommand(new Cassandra<>(this));

        bootstrap.addBundle(new SwaggerBundle<>() {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(FC configuration)
            {
                SwaggerBundleConfiguration swaggerBundleConfiguration = new SwaggerBundleConfiguration();

                swaggerBundleConfiguration.setResourcePackage("com.datastax.fallout.service.resources." +
                    (configuration.getMode() == ServerMode.RUNNER ? "runner" : "server"));
                if (configuration.getExternalUrl() != null)
                {
                    swaggerBundleConfiguration.setSchemes(new String[] {
                        URI.create(configuration.getExternalUrl()).getScheme()});
                }
                return swaggerBundleConfiguration;
            }
        });

        bootstrap.setObjectMapper(JacksonUtils.getObjectMapper());
    }

    private static void truncateTrailingSlashesInUrls(FalloutConfiguration conf)
    {
        RewriteHandler rewriteHandler = new RewriteHandler();

        rewriteHandler.addRule(new RedirectRegexRule("/(.*)/", "/$1"));

        conf.getServerFactory().insertHandler(rewriteHandler);
    }

    private static void addArtifactServlet(FalloutConfiguration conf, Environment environment,
        RewriteHandler rewriteHandler)
    {
        Function<Path, Servlet> createArtifactServlet =
            conf.useNginxToServeArtifacts() ? NginxArtifactServlet::new : JettyArtifactServlet::new;

        final ServletRegistration.Dynamic registration = environment.servlets()
            .addServlet("artifacts-servlet", createArtifactServlet.apply(
                Paths.get(conf.getArtifactPath())
            ));
        registration.addMapping("/artifacts/*");

        // Redirect direct requests to the artifacts servlet URL to make sure users see (and share) the right path
        // in the browser; fallout used to make these URLs public, so some will have been shared in tickets etc.,
        // which means we should carry on handling them.
        rewriteHandler.addRule(new RedirectRegexRule(
            "/artifacts/" +
                "(" + AccountResource.EMAIL_PATTERN + ")/" +
                "(" + TestResource.NAME_PATTERN + ")/" +
                "(" + TestResource.ID_PATTERN + ")/" +
                "(.*)",
            "/tests/ui/$1/$2/$3/artifacts/$4"));

        // Rewrite artifacts requests so that they're handled by the artifacts servlet
        rewriteHandler.addRule(new RewriteRegexRule(
            "/tests/ui/" +
                "(" + AccountResource.EMAIL_PATTERN + ")/" +
                "(" + TestResource.NAME_PATTERN + ")/" +
                "(" + TestResource.ID_PATTERN + ")/" +
                "artifacts/(.+)",
            "/artifacts/$1/$2/$3/$4"));

        environment.servlets().addMimeMapping("log", "text/plain; charset=UTF-8");
    }

    @VisibleForTesting
    public void withRunnerTestRunStatusFeed(TestRunStatusUpdatePublisher runnerTestRunStatusFeed)
    {
        this.runnerTestRunStatusFeed = Optional.of(runnerTestRunStatusFeed);
    }

    @VisibleForTesting
    public void setPreCreateSchemaCallback(Consumer<CassandraDriverManager> preCreateSchemaCallback)
    {
        this.preCreateSchemaCallback = preCreateSchemaCallback;
    }

    @VisibleForTesting
    public void setComponentFactory(ComponentFactory componentFactory)
    {
        this.componentFactory = componentFactory;
    }

    @VisibleForTesting
    public ComponentFactory getComponentFactory()
    {
        return componentFactory;
    }

    @VisibleForTesting
    public CassandraDriverManager getCassandraDriverManager()
    {
        return cassandraDriverManager;
    }

    interface CreateAbortableTestRunExecutorFactory<FC extends FalloutConfiguration>
    {
        AbortableRunnableExecutorFactory create(
            FC conf, LifecycleManager m, UserMessenger mailer,
            TestDAO testDAO, TestRunDAO testRunDAO, ActiveTestRunFactory activeTestRunFactory);
    }

    @Override
    public void run(FC conf, Environment environment) throws Exception
    {
        final int commonPoolSize = ForkJoinPool.commonPool().getParallelism();
        Verify.verify(commonPoolSize == 1024,
            "Please run with the java option " +
                "-Djava.util.concurrent.ForkJoinPool.common.parallelism=1024 (current value is %s)",
            commonPoolSize);

        final LifecycleManager m = new LifecycleManager(environment);

        httpClient = m.manage(FalloutClientBuilder
            .forEnvironment(environment)
            .build(), Client::close);

        // Prevent accidental inclusion of dependencies modifying server behaviour
        environment.jersey().enable(CommonProperties.FEATURE_AUTO_DISCOVERY_DISABLE);

        switch (conf.getMode())
        {
            case STANDALONE:
                runServer(conf, environment,
                    (CreateAbortableTestRunExecutorFactory<FC>) this::createStandaloneTestRunExecutorFactory,
                    SchemaMode.CREATE_SCHEMA);
                break;
            case QUEUE:
                runServer(conf, environment,
                    (CreateAbortableTestRunExecutorFactory<FC>) this::createQueueTestRunExecutorFactory,
                    SchemaMode.USE_EXISTING_SCHEMA);
                break;
            case RUNNER:
                runRunner(conf, environment);
                break;
        }
    }

    /** Create the {@link RunnableExecutorFactory} that will actually create the {@link ActiveTestRun}s via the specified
     * {@link ActiveTestRunFactory} and run them; used in {@link ServerMode#STANDALONE} and {@link ServerMode#RUNNER}.
     * Finished callbacks are processed by this executor, because if we try to process them in {@link ServerMode#QUEUE}
     * they could be lost due to missed updates while {@link ServerMode#QUEUE} is offline during a redeploy */
    private RunnableExecutorFactory createThreadedTestRunExecutorFactoryWithFinishedCallbacks(FC conf,
        LifecycleManager m, UserMessenger mailer, TestDAO testDAO, TestRunDAO testRunDAO,
        ActiveTestRunFactory activeTestRunFactory)
    {
        JobLoggersFactory fileLoggersFactory =
            new JobLoggersFactory(Paths.get(conf.getArtifactPath()), conf.logTestRunsToConsole());

        final var threadedTestRunExecutorFactory = new ThreadedRunnableExecutorFactory(
            fileLoggersFactory, testRunDAO::update,
            activeTestRunFactory, conf);

        final var userNotifier = new FinishedTestRunUserNotifier(conf.getExternalUrl(),
            mailer,
            SlackUserMessenger.create(conf.getSlackToken(),
                m.manage(FalloutClientBuilder.forComponent(SlackUserMessenger.class).build(), Client::close)));

        return new RunnableExecutorFactory() {
            @Override
            public RunnableExecutor create(TestRun testRun,
                UserCredentials userCredentials)
            {
                final var executor = threadedTestRunExecutorFactory.create(testRun, userCredentials);
                executor.getTestRunStatus().addFinishedCallback(() -> {
                    testDAO.increaseSizeOnDiskBytesByTestRunSize(testRun);
                    userNotifier.notify(testRun);
                });

                return executor;
            }

            @Override
            public void close()
            {
                threadedTestRunExecutorFactory.close();
            }
        };
    }

    /** Make aborting stale testruns a lifecycle event, so that we
     *  don't continue until it's completed (and fail startup if it fails). */
    private static class StaleTestRunAborter implements LifecycleManager.ManagedStartOnly
    {
        private final TestRunDAO testRunDAO;
        private final Supplier<List<ReadOnlyTestRun>> activeTestRuns;

        private StaleTestRunAborter(TestRunDAO testRunDAO, Supplier<List<ReadOnlyTestRun>> activeTestRuns)
        {
            this.testRunDAO = testRunDAO;
            this.activeTestRuns = activeTestRuns;
        }

        @Override
        public void start() throws Exception
        {
            testRunDAO.abortStaleTestRuns(activeTestRuns.get());
        }
    }

    private AbortableRunnableExecutorFactory createStandaloneTestRunExecutorFactory(
        FC conf, LifecycleManager m, UserMessenger mailer,
        TestDAO testDAO, TestRunDAO testRunDAO, ActiveTestRunFactory activeTestRunFactory)
    {
        final var abortableTestRunExecutorFactory = new AbortableRunnableExecutorFactory(
            createThreadedTestRunExecutorFactoryWithFinishedCallbacks(
                conf, m, mailer, testDAO, testRunDAO, activeTestRunFactory));

        m.manage(new StaleTestRunAborter(testRunDAO, abortableTestRunExecutorFactory::activeTestRuns));

        return abortableTestRunExecutorFactory;
    }

    private AbortableRunnableExecutorFactory createQueueTestRunExecutorFactory(
        FC conf, LifecycleManager m, UserMessenger mailer,
        TestDAO testDAO, TestRunDAO testRunDAO, ActiveTestRunFactory activeTestRunFactory)
    {
        final WebTarget delegateRunnerTarget = httpClient.target(conf.getDelegateURI().toString());

        final AbortableRunnableExecutorFactory abortableTestRunExecutorFactory = new AbortableRunnableExecutorFactory(
            m.manage(new DelegatingRunnableExecutorFactory(delegateRunnerTarget, testRunDAO::get)));

        final List<CompletableFuture<Void>> allExistingTestRunsKnown = new ArrayList<>();

        conf.getExistingRunnerURIsExcludingDelegate().forEach(uri -> {
            final var existingRunnerTestRunExecutorFactory =
                m.manage(new DelegatingExecutorFactory(httpClient.target(uri), testRunDAO::get,
                    abortableTestRunExecutorFactory::addExecutorIfNotExists));

            allExistingTestRunsKnown.add(existingRunnerTestRunExecutorFactory.waitUntilAllExistingTestRunsKnownAsync());
        });

        m.manage(new StaleTestRunAborter(testRunDAO, abortableTestRunExecutorFactory::activeTestRuns) {

            @Override
            public void start() throws Exception
            {
                logger.withScopedInfo("Waiting for all existing test runs to register").run(
                    () -> CompletableFuture.allOf(allExistingTestRunsKnown.toArray(new CompletableFuture[] {})).join());
                super.start();
            }
        });

        return abortableTestRunExecutorFactory;
    }

    @VisibleForTesting
    public int runningTestRunsCount()
    {
        return runningTestRunsCount.getAsInt();
    }

    private static class LifecycleManager
    {
        private final Environment environment;

        private LifecycleManager(Environment environment)
        {
            this.environment = environment;
        }

        public <T extends Managed> T manage(T object)
        {
            environment.lifecycle().manage(object);
            return object;
        }

        public <T> T manage(T object, Consumer<T> onClose)
        {
            manage(new AutoCloseableManager(() -> onClose.accept(object)));
            return object;
        }

        private interface ManagedStartOnly extends Managed
        {
            @Override
            default void stop()
            {
            }
        }

        /** Manage only the start lifecycle event: this is for objects that handle
         *  shutdown outside of lifecycle stop (i.e. via {@link #shutdownHandler}) */
        public <T extends Managed> T manageStartOnly(T object)
        {
            environment.lifecycle().manage((ManagedStartOnly) object::start);
            return object;
        }
    }

    private void setShutdownHandler(Environment environment, Managed testRunner, Managed serverSentEvents)
    {
        environment.lifecycle().manage(new Managed() {
            @Override
            public void start()
            {
                shutdownHandler = () -> Exceptions.runUnchecked(() -> {
                    // Order is important:

                    // Stop the runner, so that we wait for existing testruns to stop, leaving everything else running
                    // including the REST API;
                    testRunner.stop();

                    // Stop SSE so that clients do not hold connections open and prevent jetty from stopping;
                    serverSentEvents.stop();

                    // Stop clojure; this has the unwanted side-effect of _loading_ clojure if it hasn't already
                    // been loaded
                    ClojureShutdown.shutdown();

                    // Stop jetty and everything else.
                    environment.getApplicationContext().getServer().stop();
                });
            }

            @Override
            public void stop()
            {
                shutdownHandler = () -> {};
            }
        });
    }

    private List<AuthFilter<String, User>> getAuthFilters(FC conf, UserDAO userDAO)
    {
        List<AuthFilter<String, User>> filters = new ArrayList<>();

        // This will only be applied to methods/classes annotated with RolesAllowed
        final Authorizer<User> adminAuthorizer = (user, role) -> user.isAdmin() && role.equals("ADMIN");

        AuthFilter<String, User> oauthCredentialAuthFilter = new OAuthCredentialAuthFilter.Builder<User>()
            .setAuthenticator(new FalloutTokenAuthenticator(userDAO, OAUTH_REALM))
            .setAuthorizer(adminAuthorizer)
            .setPrefix(OAUTH_BEARER_TOKEN_TYPE)
            .setRealm(OAUTH_REALM)
            .buildAuthFilter();
        filters.add(oauthCredentialAuthFilter);

        AuthFilter<String, User> uiAuthFilter;
        if (conf.getAuthenticationMode() == FalloutConfiguration.AuthenticationMode.SINGLE_USER)
        {
            if (conf.getAdminUserCreds().isEmpty())
            {
                throw new RuntimeException(String.format(
                    "Cannot use %s authentication mode without specifying %s in the environment",
                    FalloutConfiguration.AuthenticationMode.SINGLE_USER, FalloutConfiguration.ADMIN_CREDS_ENV_VAR));
            }
            uiAuthFilter = new SingleUserAuthFilter(
                () -> userDAO.getUser(conf.getAdminUserCreds().get().getEmail()));
        }
        else
        {
            uiAuthFilter = new FalloutCookieAuthFilter.Builder()
                .setAuthenticator(new FalloutTokenAuthenticator(userDAO, COOKIE_NAME))
                .setAuthorizer(adminAuthorizer)
                .setPrefix(OAUTH_BEARER_TOKEN_TYPE)
                .setRealm(OAUTH_REALM)
                .buildAuthFilter();
        }
        filters.add(uiAuthFilter);

        return filters;
    }

    private void runServer(FC conf, Environment environment,
        CreateAbortableTestRunExecutorFactory createTestRunExecutorFactory, SchemaMode schemaMode) throws Exception
    {
        final LifecycleManager m = new LifecycleManager(environment);

        final ResourceReservationLocks resourceReservationLocks = new ResourceReservationLocks();

        cassandraDriverManager = m.manage(
            new CassandraDriverManager(
                conf.getCassandraHost(), conf.getCassandraPort(), conf.getKeyspace(),
                schemaMode, preCreateSchemaCallback));

        SecurityUtil securityUtil = new SecurityUtil(conf.getSecureRandomAlgorithm());

        final var userGroupMapper = createUserGroupMapper();

        UserDAO userDAO = m.manage(new UserDAO(cassandraDriverManager, securityUtil, conf.getAdminUserCreds(),
            userGroupMapper));
        TestRunDAO testRunDAO = m.manage(new TestRunDAO(cassandraDriverManager));
        TestDAO testDAO = m.manage(new TestDAO(cassandraDriverManager, testRunDAO));
        PerformanceReportDAO reportDAO = m.manage(new PerformanceReportDAO(cassandraDriverManager));

        ActiveTestRunFactory activeTestRunFactory = createActiveTestRunFactory(conf);

        UserMessenger mailer = HtmlMailUserMessenger.create(conf);

        UserCredentialsFactory userCredentialsFactory = (testRun) -> {
            User user = userDAO.getUser(testRun.getOwner());
            if (user == null)
            {
                throw new RuntimeException(String.format("Couldn't find User with email '%s'", testRun.getOwner()));
            }
            return new UserCredentials(user, userDAO.getCIUserByUser(user));
        };

        QueuingTestRunner testRunner = m.manageStartOnly(new QueuingTestRunner(
            testRunDAO::update,
            testDAO::updateLastRunAt,
            new PersistentPendingQueue(testRunDAO::getQueued),
            userCredentialsFactory,
            createTestRunExecutorFactory.create(
                conf, m, mailer, testDAO, testRunDAO, activeTestRunFactory),
            testRun -> activeTestRunFactory.getResourceRequirements(testRun, userCredentialsFactory),
            resourceReservationLocks,
            conf.getResourceLimits(),
            conf.getStartPaused()));

        // monitor queue metrics
        QueueMetricsManager.registerMetrics(environment.metrics(), testRunDAO);

        runningTestRunsCount = testRunner::getRunningTestRunsCount;

        //Make sure the performance_reports dir exists
        FileUtils.createDirs(Paths.get(conf.getArtifactPath(), "performance_reports"));

        final HashedWheelTimer timer =
            m.manage(new HashedWheelTimer(new NamedThreadFactory("ServiceTimer")), HashedWheelTimer::stop);

        Path artifactPath = Paths.get(conf.getArtifactPath());
        final var runningTaskLock = new ReentrantLock();

        ArtifactScrubber artifactScrubber = m.manage(new ArtifactScrubber(conf.getStartPaused(), timer,
            runningTaskLock,
            Duration.hours(0), Duration.hours(24),
            artifactPath, testRunDAO, userDAO));
        ArtifactCompressor artifactCompressor = m.manage(new ArtifactCompressor(conf.getStartPaused(), timer,
            runningTaskLock, Duration.hours(12), Duration.hours(24),
            artifactPath, testRunDAO, testDAO));

        QueueAdminTask queueAdminTask = new QueueAdminTask(testRunner, List.of(artifactScrubber, artifactCompressor));
        environment.admin().addTask(queueAdminTask);

        final var artifactUsageAdminTask = new ArtifactUsageAdminTask(testRunDAO);
        environment.admin().addTask(artifactUsageAdminTask);

        environment.admin().addTask(new ArtifactCompressorAdminTask(artifactCompressor));

        environment.admin().addTask(new ShutdownTask(this::shutdown));

        truncateTrailingSlashesInUrls(conf);

        final RewriteHandler rewriteHandler = new RewriteHandler();
        conf.getServerFactory().insertHandler(rewriteHandler);

        addArtifactServlet(conf, environment, rewriteHandler);

        // Add CORS headers so that fallout API can be consumed from other than the main URL
        // The `CrossOriginFilter` comes with the required default settings: allow any origin
        environment.servlets()
            .addFilter("CORS", CrossOriginFilter.class)
            .addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");

        // Register our exception mappers: note that we must use concrete (i.e. non-generic) classes, otherwise
        // the convoluted exception mapping code in Jersey will fail with an HTTP 500 error.

        // Dropwizard registers several helpful exception mappers on server start in its ExceptionMapperBinder; one of
        // these provides a helper logging method for detecting a particular developer error when POSTing
        // forms.  Unfortunately, this intercepts _all_ IllegalStateExceptions.  We insert our IllegalStateException
        // mapper here to pre-empt it, whilest keeping the helper logic.
        environment.jersey().register(new FalloutExceptionMapper<IllegalStateException>() {
            private final IllegalStateExceptionMapper dropWizardIllegalStateExceptionMapper =
                new IllegalStateExceptionMapper();

            /** If the helper code in {@link IllegalStateExceptionMapper} applies, use that */
            @Override
            public Response toResponse(IllegalStateException exception)
            {
                if (LocalizationMessages.FORM_PARAM_CONTENT_TYPE_ERROR().equals(exception.getMessage()))
                {
                    return dropWizardIllegalStateExceptionMapper.toResponse(exception);
                }

                return super.toResponse(exception);
            }
        });

        // This is our default exception mapper.
        environment.jersey().register(new FalloutExceptionMapper<>() {});

        environment.jersey().register(new AuthDynamicFeature(new ChainedAuthFilter(getAuthFilters(conf, userDAO))));
        // Enable @RolesAllowed annotations
        environment.jersey().register(new RolesAllowedDynamicFeature());
        // If you want to use @Auth to inject a custom Principal type into your resource
        environment.jersey().register(new AuthValueFactoryProvider.Binder<>(User.class));

        final ComponentResource componentResource = new ComponentResource(conf, componentFactory);
        MainView mainView = new MainView(componentResource.getComponentTypes(), testRunner,
            addVersionedAssetsRewriteRule(rewriteHandler));
        componentResource.setMainView(mainView);

        CommandExecutor commandExecutor = new LocalCommandExecutor();

        environment.jersey().register(new StatusResource(testRunner));
        environment.jersey().register(new HomeResource(conf, userDAO, testRunDAO, testRunner,
            conf.getResourceLimits(), mainView, userGroupMapper));
        environment.jersey().register(new AdminResource(testRunner, queueAdminTask, artifactUsageAdminTask,
            mainView));
        environment.jersey().register(new AccountResource(userDAO, conf, mailer, mainView, securityUtil,
            userGroupMapper));
        environment.jersey().register(new TestResource(conf, testDAO, testRunDAO, activeTestRunFactory,
            userCredentialsFactory, reportDAO, testRunner, mainView, userGroupMapper));
        environment.jersey().register(componentResource);
        environment.jersey()
            .register(new PerformanceToolResource(testDAO, testRunDAO, reportDAO, conf.getArtifactPath(),
                mainView, userGroupMapper));

        registerOptionalResources(conf, environment, testRunDAO, commandExecutor);

        // Using SSE (which is what LiveResource uses) doesn't work unless we prevent the
        // GZIP output filter from flushing-on-demand (if we don't do this, data is queued up until the
        // GZIP implementation decides it's a good time to flush: see java.util.zip.Deflater#SYNC_FLUSH).
        ((DefaultServerWithHandlerFactory) conf.getServerFactory()).getGzipFilterFactory().setSyncFlush(true);

        final ArtifactWatcher artifactWatcher = new ArtifactWatcher(Paths.get(conf.getArtifactPath()), timer,
            conf.getArtifactWatcherCoalescingIntervalSeconds());
        environment.lifecycle().manage(artifactWatcher);

        final ServerSentEvents serverSentEvents =
            m.manageStartOnly(new ServerSentEvents(timer, conf.getServerSentEventsHeartBeatIntervalSeconds()));

        environment.jersey().register(new LiveResource(testRunDAO, artifactWatcher, serverSentEvents));

        setShutdownHandler(environment, testRunner, serverSentEvents);
    }

    protected void registerOptionalResources(FC conf, Environment environment,
        TestRunDAO testRunDAO, CommandExecutor commandExecutor)
    {
    }

    protected UserGroupMapper createUserGroupMapper()
    {
        return UserGroupMapper.empty();
    }

    private ActiveTestRunFactory createActiveTestRunFactory(FC conf)
    {
        return new ActiveTestRunFactory(conf)
            .withComponentFactory(componentFactory)
            .withResourceChecker(getResourceChecker())
            .withPostSetupHook(getEnsemblePostSetupHook());
    }

    protected Function<Ensemble, List<CompletableFuture<Boolean>>> getResourceChecker()
    {
        return ensemble -> List.of();
    }

    protected Function<Ensemble, Boolean> getEnsemblePostSetupHook()
    {
        return ensemble -> true;
    }

    /** By definition, the runner listens on a random available port on localhost: this is not
     *  intended to be configurable, and instead is communicated to whatever starts this (i.e.
     *  falloutctl) via {@link FalloutConfiguration#getPortFile()}. */
    private void configureRunnerListenPort(FC conf, ObjectMapper objectMapper)
    {
        final DefaultServerFactory serverFactory = (DefaultServerFactory) conf.getServerFactory();

        // Load the existing config, and override the settings
        final HttpConnectorFactory connectorFactory =
            Exceptions.getUnchecked(() -> objectMapper.readValue(
                objectMapper.writeValueAsString(serverFactory.getApplicationConnectors().get(0)),
                HttpConnectorFactory.class));

        // setPort(0) => use a random available port number
        connectorFactory.setPort(0);
        connectorFactory.setBindHost("localhost");

        // The runner is running on the same machine as the server: set the idle timeout
        // fairly low.  This is particularly important for /status, since this is the
        // time between sending an update on a blocked connection and continuing.
        connectorFactory.setIdleTimeout(io.dropwizard.util.Duration.milliseconds(Math.min(
            connectorFactory.getIdleTimeout().toMilliseconds(), 5000)));

        serverFactory.setApplicationConnectors(List.of(connectorFactory));
        serverFactory.setAdminConnectors(List.of(connectorFactory));
    }

    private void runRunner(FC conf, Environment environment)
    {
        final LifecycleManager m = new LifecycleManager(environment);

        configureRunnerListenPort(conf, environment.getObjectMapper());

        cassandraDriverManager = m.manage(
            new CassandraDriverManager(
                conf.getCassandraHost(), conf.getCassandraPort(), conf.getKeyspace(),
                SchemaMode.CREATE_SCHEMA, preCreateSchemaCallback));

        final TestRunDAO testRunDAO = m.manage(new TestRunDAO(cassandraDriverManager));
        final TestDAO testDAO = m.manage(new TestDAO(cassandraDriverManager, testRunDAO));

        ActiveTestRunFactory activeTestRunFactory = createActiveTestRunFactory(conf);

        TestRunStatusUpdatePublisher testRunStatusUpdatePublisher = runnerTestRunStatusFeed
            .orElse(new TestRunStatusUpdatePublisher());

        final HashedWheelTimer timer = m.manage(
            new HashedWheelTimer(new NamedThreadFactory("ServiceTimer")), HashedWheelTimer::stop);

        final ServerSentEvents serverSentEvents = m.manageStartOnly(
            new ServerSentEvents(timer, conf.getServerSentEventsHeartBeatIntervalSeconds()));

        final DirectTestRunner testRunner = m.manageStartOnly(
            new DirectTestRunner(
                createThreadedTestRunExecutorFactoryWithFinishedCallbacks(
                    conf, m, HtmlMailUserMessenger.create(conf), testDAO,
                    testRunDAO, activeTestRunFactory),
                this::shutdown,
                testRunStatusUpdatePublisher));

        runningTestRunsCount = testRunner::getRunningTestRunsCount;

        environment.jersey().register(new RunnerResource(testRunner, serverSentEvents, testRunStatusUpdatePublisher));

        setShutdownHandler(environment, testRunner, serverSentEvents);
    }

    public void shutdown()
    {
        shutdownHandler.run();
    }
}
