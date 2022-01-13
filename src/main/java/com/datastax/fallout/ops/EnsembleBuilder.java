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
package com.datastax.fallout.ops;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import com.datastax.fallout.harness.NullTestRunAbortedStatus;
import com.datastax.fallout.harness.NullTestRunLinkUpdater;
import com.datastax.fallout.harness.TestRunAbortedStatus;
import com.datastax.fallout.harness.TestRunLinkUpdater;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory.TestRunScratchSpace;

import static java.util.stream.Stream.concat;

/**
 * A Builder class for Ensemble. Optional but useful
 */
public class EnsembleBuilder
{
    public static final String CONTROLLER_NAME = "controller";
    public static final String OBSERVER_NAME = "observer";

    private UUID testRunId;
    private List<NodeGroupBuilder> serverBuilders = new LinkedList<>();
    private List<NodeGroupBuilder> clientBuilders = new LinkedList<>();
    private NodeGroupBuilder observerBuilder;
    private NodeGroupBuilder controllerBuilder;
    private TestRunAbortedStatus testRunAbortedStatus = new NullTestRunAbortedStatus();
    private JobLoggers loggers = new JobConsoleLoggers();
    private EnsembleCredentials credentials;
    private TestRunLinkUpdater testRunLinkUpdater = new NullTestRunLinkUpdater();
    private LocalFilesHandler localFilesHandler = LocalFilesHandler.empty();

    public static EnsembleBuilder create()
    {
        return new EnsembleBuilder();
    }

    public EnsembleBuilder withTestRunId(UUID testRunId)
    {
        this.testRunId = testRunId;
        return this;
    }

    public EnsembleBuilder withServerGroup(NodeGroupBuilder serverBuilder)
    {
        Preconditions.checkArgument(serverBuilder != null, "server group must be non-null");

        serverBuilder.withRole(Ensemble.Role.SERVER);
        this.serverBuilders.add(serverBuilder);

        return this;
    }

    public EnsembleBuilder withClientGroup(NodeGroupBuilder clientBuilder)
    {
        Preconditions.checkArgument(clientBuilder != null, "client group must be non-null");

        clientBuilder.withRole(Ensemble.Role.CLIENT);
        this.clientBuilders.add(clientBuilder);

        return this;
    }

    public Optional<NodeGroupBuilder> findServerGroup(String name)
    {
        if ("server".equalsIgnoreCase(name) && serverBuilders.size() == 1)
        {
            return Optional.of(serverBuilders.get(0));
        }
        return serverBuilders.stream().filter(s -> s.getName().equalsIgnoreCase(name)).findFirst();
    }

    public Optional<NodeGroupBuilder> findClientGroup(String name)
    {
        if ("client".equalsIgnoreCase(name) && clientBuilders.size() == 1)
        {
            return Optional.of(clientBuilders.get(0));
        }
        return clientBuilders.stream().filter(s -> s.getName().equalsIgnoreCase(name)).findFirst();
    }

    public EnsembleBuilder withObserverGroup(NodeGroupBuilder observerGroup)
    {
        Preconditions.checkArgument(observerGroup != null, "observer group must be non-null");
        Preconditions.checkArgument(this.observerBuilder == null, "observer group already set");

        observerGroup.withRole(Ensemble.Role.OBSERVER);
        this.observerBuilder = observerGroup;

        return this;
    }

    public EnsembleBuilder withControllerGroup(NodeGroupBuilder controllerGroup)
    {
        Preconditions.checkArgument(controllerGroup != null, "controller group must be non-null");
        Preconditions.checkArgument(this.controllerBuilder == null, "controller group already set");

        controllerGroup.withRole(Ensemble.Role.CONTROLLER);
        this.controllerBuilder = controllerGroup;

        return this;
    }

    public EnsembleBuilder withDefaultObserverGroup(NodeGroupBuilder observerBuilder)
    {
        if (this.observerBuilder == null)
        {
            observerBuilder.withRole(Ensemble.Role.OBSERVER);
            this.observerBuilder = observerBuilder;
        }
        return this;
    }

    public EnsembleBuilder withDefaultControllerGroup(NodeGroupBuilder controllerBuilder)
    {
        if (this.controllerBuilder == null)
        {
            controllerBuilder.withRole(Ensemble.Role.CONTROLLER);
            this.controllerBuilder = controllerBuilder;
        }
        return this;
    }

    public EnsembleBuilder withTestRunAbortedStatus(TestRunAbortedStatus testRunAbortedStatus)
    {
        this.testRunAbortedStatus = testRunAbortedStatus;
        return this;
    }

    public EnsembleBuilder withLoggers(JobLoggers loggers)
    {
        this.loggers = loggers;
        return this;
    }

    public EnsembleBuilder withCredentials(EnsembleCredentials credentials)
    {
        this.credentials = credentials;
        return this;
    }

    public EnsembleBuilder withTestRunLinkUpdater(TestRunLinkUpdater testRunLinkUpdater)
    {
        this.testRunLinkUpdater = testRunLinkUpdater;
        return this;
    }

    public EnsembleBuilder withLocalFilesHandler(LocalFilesHandler localFilesHandler)
    {
        this.localFilesHandler = localFilesHandler;
        return this;
    }

    private void check()
    {
        Preconditions.checkArgument(!serverBuilders.isEmpty(), "Server Builders are missing");
        Preconditions.checkArgument(!clientBuilders.isEmpty(), "Client Builders are missing");
        Preconditions.checkNotNull(observerBuilder, "Observer Builder is missing");
        Preconditions.checkNotNull(controllerBuilder, "Controller Builder is missing");

        checkUniqueNames(serverBuilders, clientBuilders);
    }

    private static void checkUniqueNames(List<NodeGroupBuilder> servers, List<NodeGroupBuilder> clients)
    {
        Map<String, NodeGroupBuilder> seenNames = new HashMap<>();
        Consumer<NodeGroupBuilder> checkUnique = b -> {
            String name = b.getName().toLowerCase();
            // check for identity since you can link to the exact same NodeGroupBuilder instance
            Preconditions.checkArgument(!seenNames.containsKey(name) || seenNames.get(name) == b,
                "Duplicate node group name: " + name);
            seenNames.put(name, b);
        };
        concat(servers.stream(), clients.stream()).forEach(checkUnique);
    }

    public static IntSupplier createNodeOrdinalSupplier()
    {
        return new IntSupplier() {
            private int current = 0;

            @Override
            public int getAsInt()
            {
                return this.current++;
            }
        };
    }

    public Ensemble build(Path testRunArtifactPath, TestRunScratchSpace testRunScratchSpace)
    {
        check();

        final IntSupplier nodeOrdinalSupplier = createNodeOrdinalSupplier();

        Stream
            .of(serverBuilders.stream(), clientBuilders.stream(), Stream.of(observerBuilder, controllerBuilder))
            .flatMap(Function.identity())
            .forEach(nodeGroupBuilder -> {
                nodeGroupBuilder.withLoggers(loggers)
                    .withTestRunAbortedStatus(testRunAbortedStatus)
                    .withEnsembleOrdinalSupplier(nodeOrdinalSupplier)
                    .withTestRunArtifactPath(testRunArtifactPath)
                    .withCredentials(credentials)
                    .withTestRunScratchSpace(testRunScratchSpace)
                    .withExtraAvailableProviders(localFilesHandler);
            });

        observerBuilder.withName(OBSERVER_NAME);
        controllerBuilder.withName(CONTROLLER_NAME);

        List<NodeGroup> servers = serverBuilders.stream().map(NodeGroupBuilder::build).toList();
        List<NodeGroup> clients = clientBuilders.stream().map(NodeGroupBuilder::build).toList();

        NodeGroup observers = observerBuilder.build();
        NodeGroup controllers = controllerBuilder.build();

        return new Ensemble(testRunId, servers, clients, observers, controllers, testRunLinkUpdater,
            testRunScratchSpace.makeScratchSpaceForWorkload(), loggers.getShared(), localFilesHandler);
    }
}
