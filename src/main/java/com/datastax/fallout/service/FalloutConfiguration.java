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

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import io.dropwizard.Configuration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.logging.AppenderFactory;
import io.dropwizard.logging.DefaultLoggingFactory;
import io.dropwizard.logging.FileAppenderFactory;
import io.dropwizard.logging.LoggerConfiguration;
import io.dropwizard.logging.LoggingFactory;
import io.dropwizard.metrics.graphite.GraphiteReporterFactory;
import io.dropwizard.request.logging.LogbackAccessRequestLogFactory;
import io.dropwizard.server.DefaultServerFactory;

import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.JobFileLoggers;
import com.datastax.fallout.ops.PropertyBasedComponent;
import com.datastax.fallout.runner.ResourceLimit;
import com.datastax.fallout.service.auth.SecurityUtil;
import com.datastax.fallout.service.core.GrafanaTenantUsageData;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;

/**
 * Fallout configuration file
 */
public class FalloutConfiguration extends Configuration
{
    public static final String PID_FILE = "fallout.pid";
    public static final String PORT_FILE = "fallout.port";

    public static final Path SERVER_LOG_DIR = Paths.get("logs");
    public static final Path SERVER_RUN_DIR = Paths.get("run");
    public static final String RUNNERS_SUB_DIR = "runners";

    public static final String ADMIN_CREDS_ENV_VAR = "FALLOUT_ADMIN_CREDS";

    @JsonProperty
    private Path falloutHome;

    @NotEmpty
    @JsonProperty
    private String cassandraHost =
        System.getenv().getOrDefault("FALLOUT_CASSANDRA_HOST", "localhost");

    @Min(1)
    @Max(65535)
    @JsonProperty
    private Integer cassandraPort =
        Integer.parseInt(System.getenv().getOrDefault("FALLOUT_CASSANDRA_PORT", "9096"));

    /**
     * Logic changes based on this flag
     * @see PropertyBasedComponent#disabledWhenShared()
     */
    @JsonProperty
    private boolean isSharedEndpoint = false;

    /** What mode the FalloutService will run in; defaults to {@link ServerMode#STANDALONE}, set to {@link ServerMode#RUNNER} by
     *  calling {@link #setRunnerMode} */
    public enum ServerMode
    {
        STANDALONE, QUEUE, RUNNER
    }

    @JsonIgnore
    private Optional<Integer> runnerId = Optional.empty();

    /**
     * Only allows people with datastax emails to register
     */
    @JsonProperty
    private boolean datastaxOnly = false;

    /** A list of domains (in the form '@example.com') to hide in
     *  displayed emails in contexts where it makes sense to do so */
    @JsonProperty
    protected List<String> hideDisplayedEmailDomains = List.of();

    /** Forces local commands to use Java 8: for testing fallout itself only */
    @JsonProperty
    private boolean forceJava8ForLocalCommands = false;

    @NotEmpty
    @JsonProperty
    private String keyspace = "dev";

    @JsonProperty
    private String artifactPath = Paths.get("", "tests").toAbsolutePath().toString();

    @JsonProperty
    private boolean useNginxToServeArtifacts = false;

    @JsonProperty
    private String smtpFrom;

    @JsonProperty
    private String smtpUser;

    @JsonProperty
    private String smtpPass;

    @JsonProperty
    private String smtpHost;

    @JsonProperty
    @Min(1)
    @Max(65535)
    private Integer smtpPort = 25;

    @JsonProperty
    private String slackToken;

    @NotEmpty
    @JsonProperty
    private String externalUrl = "http://localhost:8080";

    @JsonProperty
    private boolean useTeamOpenstackCredentials = false;

    @JsonProperty
    private boolean logTestRunsToConsole = false;

    @JsonProperty
    private int serverSentEventsHeartBeatIntervalSeconds = 10;

    @JsonProperty
    private int artifactWatcherCoalescingIntervalSeconds = 1;

    @JsonProperty
    private String secureRandomAlgorithm = SecurityUtil.DEFAULT_ALGORITHM;

    @JsonProperty
    private boolean startPaused = false;

    @JsonIgnore
    private Optional<Integer> delegateRunnerId = Optional.empty();

    @JsonProperty
    private List<GrafanaTenantUsageData> grafanaTenantUsageData;

    @JsonIgnore
    private Optional<String> defaultAdminCreds = Optional.ofNullable(System.getenv(ADMIN_CREDS_ENV_VAR));

    @JsonProperty
    private List<ResourceLimit> resourceLimits = List.of();

    public enum AuthenticationMode
    {
        SINGLE_USER,
        MULTI_USER
    }

    @JsonProperty
    private AuthenticationMode authenticationMode = Optional
        .ofNullable(System.getenv("FALLOUT_AUTH_MODE"))
        .map(String::toUpperCase)
        .map(AuthenticationMode::valueOf)
        .orElse(AuthenticationMode.MULTI_USER);

    public Boolean getIsSharedEndpoint()
    {
        return isSharedEndpoint;
    }

    @JsonIgnore
    public void setRunnerMode(int runnerId)
    {
        this.runnerId = Optional.of(runnerId);
    }

    @JsonIgnore
    public int getDelegateRunnerId()
    {
        Preconditions.checkState(delegateRunnerId.isPresent());
        return delegateRunnerId.get();
    }

    @JsonIgnore
    public void setQueueMode(int delegateRunnerId)
    {
        this.delegateRunnerId = Optional.of(delegateRunnerId);
    }

    @JsonIgnore
    public ServerMode getMode()
    {
        return delegateRunnerId.isPresent() ?
            ServerMode.QUEUE :
            runnerId.isPresent() ?
                ServerMode.RUNNER :
            ServerMode.STANDALONE;
    }

    @VisibleForTesting
    public void setFalloutHome(Path falloutHome)
    {
        this.falloutHome = falloutHome;
    }

    private Path getFalloutHome()
    {
        if (falloutHome != null)
        {
            return falloutHome;
        }
        String falloutHomeEnv = System.getenv("FALLOUT_HOME");
        if (falloutHomeEnv != null)
        {
            return Paths.get(falloutHomeEnv);
        }
        return Paths.get("");
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public String getExternalUrl()
    {
        return externalUrl;
    }

    public String getArtifactPath()
    {
        return artifactPath;
    }

    @VisibleForTesting
    public void setArtifactPath(String artifactPath)
    {
        this.artifactPath = artifactPath;
    }

    public boolean useNginxToServeArtifacts()
    {
        return useNginxToServeArtifacts;
    }

    public String getCassandraHost()
    {
        return cassandraHost;
    }

    public Integer getCassandraPort()
    {
        return cassandraPort;
    }

    public void setKeyspace(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public String getSmtpFrom()
    {
        return smtpFrom;
    }

    public void setSmtpFrom(String smtpFrom)
    {
        this.smtpFrom = smtpFrom;
    }

    public String getSmtpUser()
    {
        return smtpUser;
    }

    public String getSmtpPass()
    {
        return smtpPass;
    }

    public String getSlackToken()
    {
        return slackToken;
    }

    public String getSmtpHost()
    {
        return smtpHost;
    }

    public Integer getSmtpPort()
    {
        return smtpPort;
    }

    public boolean isDatastaxOnly()
    {
        return datastaxOnly;
    }

    public String hideDisplayedEmailDomains(String input)
    {
        for (var domain : hideDisplayedEmailDomains)
        {
            input = input.replace(domain, "");
        }
        return input;
    }

    public boolean forceJava8ForLocalCommands()
    {
        return forceJava8ForLocalCommands;
    }

    public boolean getUseTeamOpenstackCredentials()
    {
        return useTeamOpenstackCredentials;
    }

    public boolean logTestRunsToConsole()
    {
        return logTestRunsToConsole;
    }

    public int getServerSentEventsHeartBeatIntervalSeconds()
    {
        return serverSentEventsHeartBeatIntervalSeconds;
    }

    public int getArtifactWatcherCoalescingIntervalSeconds()
    {
        return artifactWatcherCoalescingIntervalSeconds;
    }

    public String getSecureRandomAlgorithm()
    {
        return secureRandomAlgorithm;
    }

    public Path getLogDir(Optional<Integer> runnerId)
    {
        return getFalloutHome().resolve(runnerId
            .map(runnerId_ -> SERVER_LOG_DIR.resolve(RUNNERS_SUB_DIR).resolve(String.valueOf(runnerId_)))
            .orElse(SERVER_LOG_DIR));
    }

    @JsonIgnore
    private Path getLogDir()
    {
        return getLogDir(runnerId);
    }

    private static Path getRunDir(Path falloutHome, Optional<Integer> runnerId)
    {
        return falloutHome.resolve(runnerId
            .map(runnerId_ -> SERVER_RUN_DIR.resolve(RUNNERS_SUB_DIR).resolve(String.valueOf(runnerId_)))
            .orElse(SERVER_RUN_DIR));
    }

    public Path getRunDir(Optional<Integer> runnerId)
    {
        return getRunDir(getFalloutHome(), runnerId);
    }

    @JsonIgnore
    public Path getRunDir()
    {
        return getRunDir(runnerId);
    }

    @JsonIgnore
    public Path getToolsDir()
    {
        return Optional.ofNullable(System.getenv("FALLOUT_TOOLS_DIR"))
            .map(Paths::get)
            .orElseGet(() -> getRunDir().resolve("tools"));
    }

    public static Path getPidFile(Path falloutHome, Optional<Integer> runnerId)
    {
        return getRunDir(falloutHome, runnerId).resolve(PID_FILE);
    }

    public Path getPidFile(Optional<Integer> runnerId)
    {
        return getPidFile(getFalloutHome(), runnerId);
    }

    @JsonIgnore
    public Path getPidFile()
    {
        return getPidFile(runnerId);
    }

    public static Path getPortFile(Path falloutHome, int runnerId)
    {
        return getRunDir(falloutHome, Optional.of(runnerId)).resolve(PORT_FILE);
    }

    @JsonIgnore
    public Path getPortFile()
    {
        Preconditions.checkState(runnerId.isPresent());
        return getPortFile(getFalloutHome(), runnerId.get());
    }

    private static int readPortFromPortFile(Path portFile)
    {
        return Exceptions.getUncheckedIO(() -> Integer.parseInt(Files.readString(portFile)));
    }

    private static URI getRunnerURI(int port)
    {
        return Exceptions.getUnchecked(() -> new URL("http", "localhost", port, "/").toURI());
    }

    @JsonIgnore
    public URI getDelegateURI()
    {
        return getRunnerURI(readPortFromPortFile(getFalloutHome()
            .resolve(SERVER_RUN_DIR)
            .resolve(RUNNERS_SUB_DIR)
            .resolve(String.valueOf(getDelegateRunnerId()))
            .resolve(PORT_FILE)));
    }

    @JsonIgnore
    public Stream<URI> getExistingRunnerURIsExcludingDelegate()
    {
        return FileUtils.listDir(getFalloutHome().resolve(SERVER_RUN_DIR.resolve(RUNNERS_SUB_DIR)))
            .stream()
            .filter(Files::isDirectory)
            .filter(dir -> {
                try
                {
                    return Integer.parseInt(dir.getFileName().toString()) != getDelegateRunnerId();
                }
                catch (NumberFormatException e)
                {
                    throw new RuntimeException(
                        String.format("Non-integer directory %s found in runners directory", dir));
                }
            })
            .map(path -> path.resolve(PORT_FILE))
            .filter(Files::isRegularFile)
            .map(FalloutConfiguration::readPortFromPortFile)
            .map(FalloutConfiguration::getRunnerURI);
    }

    @JsonIgnore
    public String getMetricsSuffix()
    {
        return runnerId.map(runnerId_ -> "." + runnerId_).orElse("");
    }

    public boolean getStartPaused()
    {
        return startPaused;
    }

    public List<GrafanaTenantUsageData> getGrafanaTenantUsageData()
    {
        return grafanaTenantUsageData;
    }

    public List<ResourceLimit> getResourceLimits()
    {
        return resourceLimits;
    }

    @AutoValue
    public static abstract class UserCreds
    {
        /** Expects a string in the form "username:email:password"
         */
        public static UserCreds from(String creds)
        {
            var ownerEmailPassword = Splitter.on(":").splitToList(creds);
            if (ownerEmailPassword.size() != 3)
            {
                throw new InvalidConfigurationException(String.format(
                    "Incorrect format for FALLOUT_ADMIN_CREDS (%s): must be \"USERNAME:EMAIL:PASSWORD\"", creds));
            }
            return new AutoValue_FalloutConfiguration_UserCreds(
                ownerEmailPassword.get(0),
                ownerEmailPassword.get(1),
                ownerEmailPassword.get(2));
        }

        public abstract String getName();

        public abstract String getEmail();

        public abstract String getPassword();
    }

    /** Reads the environment variable {@code FALLOUT_ADMIN_CREDS} assuming the format
     *  "username:email:password", and returns a CreateUser instance if a valid string
     *  is found.  If non-empty and invalid, throws an InvalidConfigurationException.
     *
     *  This enables relatively secure setup of a new installation.
     */
    @JsonIgnore
    public Optional<UserCreds> getAdminUserCreds()
    {
        return defaultAdminCreds.map(UserCreds::from);
    }

    @JsonIgnore
    @VisibleForTesting
    public void setAdminUserCreds(String creds)
    {
        this.defaultAdminCreds = Optional.of(creds);
    }

    public AuthenticationMode getAuthenticationMode()
    {
        return authenticationMode;
    }

    public void setAuthenticationMode(AuthenticationMode authenticationMode)
    {
        this.authenticationMode = authenticationMode;
    }

    @Valid
    @NotNull
    private ServerWithHandlerFactory server = new DefaultServerWithHandlerFactory();

    @JsonProperty("server")
    @Override
    public ServerWithHandlerFactory getServerFactory()
    {
        return server;
    }

    @JsonProperty("server")
    public void setFalloutServerFactory(ServerWithHandlerFactory factory)
    {
        this.server = factory;
    }

    private <A extends AppenderFactory<?>> void modifyAppenders(Class<A> clazz,
        List<? extends AppenderFactory<?>> appenders,
        Consumer<A> appenderModifier)
    {
        appenders.stream()
            .filter(clazz::isInstance)
            .map(clazz::cast)
            .forEach(appenderModifier);
    }

    /** Updates all configured loggers with the current setting of {@link #getLogDir()} */
    private <A extends AppenderFactory<?>> void modifyAppLogAppenders(Class<A> clazz, Consumer<A> appenderModifier)
    {
        final LoggingFactory loggingFactory = getLoggingFactory();
        final var objectMapper = Jackson.newObjectMapper();

        if (loggingFactory instanceof DefaultLoggingFactory)
        {
            // Root appenders will have been defined as AppenderFactories
            final var defaultLoggingFactory = (DefaultLoggingFactory) loggingFactory;
            modifyAppenders(clazz, defaultLoggingFactory.getAppenders(), appenderModifier);

            // Appenders on individual loggers will be defined as JsonNodes;
            // see {@link DefaultLoggingFactory#configureLoggers}
            for (var nameAndNode : defaultLoggingFactory.getLoggers().entrySet())
            {
                final var jsonNode = nameAndNode.getValue();
                if (!jsonNode.isObject())
                {
                    continue;
                }

                final LoggerConfiguration configuration;

                try
                {
                    configuration = objectMapper.treeToValue(jsonNode, LoggerConfiguration.class);
                }
                catch (JsonProcessingException e)
                {
                    throw new IllegalArgumentException("Wrong format of logger '" + nameAndNode.getKey() + "'", e);
                }

                modifyAppenders(clazz, configuration.getAppenders(), appenderModifier);

                nameAndNode.setValue(objectMapper.valueToTree(configuration));
            }
        }
    }

    /** Updates all configured loggers with a fixed consistent format if no format has been set */
    public void updateAppLogFormat()
    {
        modifyAppLogAppenders(AbstractAppenderFactory.class, appender -> {
            appender.setLogFormat(JobFileLoggers.FALLOUT_PATTERN);
        });
    }

    private void updateLogFilename(Supplier<String> getter, Consumer<String> setter)
    {
        String logFileName = getter.get();
        if (logFileName != null && !logFileName.isBlank())
        {
            setter.accept(getLogDir().resolve(Paths.get(logFileName).getFileName()).toString());
        }
    }

    /** Updates all configured loggers with the current setting of {@link #getLogDir()} */
    public void updateLogDir()
    {
        modifyAppLogAppenders(FileAppenderFactory.class, appender -> {
            updateLogFilename(appender::getCurrentLogFilename, appender::setCurrentLogFilename);
            updateLogFilename(appender::getArchivedLogFilenamePattern, appender::setArchivedLogFilenamePattern);
        });

        final var serverFactory = (DefaultServerFactory) getServerFactory();
        final var requestLogFactory = (LogbackAccessRequestLogFactory) serverFactory.getRequestLogFactory();
        modifyAppenders(FileAppenderFactory.class, requestLogFactory.getAppenders(), appender -> {
            updateLogFilename(appender::getCurrentLogFilename, appender::setCurrentLogFilename);
            updateLogFilename(appender::getArchivedLogFilenamePattern, appender::setArchivedLogFilenamePattern);
        });
    }

    /** Disables all configured loggers and sets logging to console */
    public void forceLoggingToConsoleOnly()
    {
        setLoggingFactory(new DefaultLoggingFactory());
    }

    /** Updates all configured metrics reporters with {@link #getMetricsSuffix} */
    public void updateMetricsSuffix()
    {
        getMetricsFactory().getReporters().stream()
            .filter(reporterFactory -> reporterFactory instanceof GraphiteReporterFactory)
            .map(reporterFactory -> (GraphiteReporterFactory) reporterFactory)
            .forEach(reporterFactory -> reporterFactory.setPrefix(
                reporterFactory.getPrefix() + getMetricsSuffix()));
    }
}
