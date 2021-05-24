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
package com.datastax.fallout.components.kubernetes;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import com.google.auto.service.AutoService;

import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.utils.FileUtils;

@AutoService(ConfigurationManager.class)
public class KubernetesLogsConfigurationManager extends ConfigurationManager
{
    private final String prefix = "fallout.configuration.management.k8s.logs.";
    private final String name = "kubernetes_logs";
    private final String description = "Collects logs at the end of a test run from a kubernetes cluster";

    private final PropertySpec<String> containerLogsNamespaceSpec = PropertySpecBuilder.createStr(prefix)
        .name("container_logs_namespace")
        .description("Collect the logs from all of the pods in the given namespace")
        .build();

    private final PropertySpec<Boolean> ignoreFailuresSpec = PropertySpecBuilder.createBool(prefix)
        .name("ignore_failures")
        .description("Ignore failures while downloading the logs")
        .defaultOf(false)
        .build();

    private final PropertySpec<Boolean> capturePreviousContainerLogsSpec = PropertySpecBuilder.createBool(prefix)
        .name("capture_previous_container_logs")
        .description("Collect the logs for the previous execution of each container (kubectrl log --previous)")
        .defaultOf(false)
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String description()
    {
        return description;
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(containerLogsNamespaceSpec, ignoreFailuresSpec, capturePreviousContainerLogsSpec);
    }

    private interface LogFailure
    {
        void log(String msg, Object... args);
    }

    @Override
    public boolean prepareArtifactsImpl(Node node)
    {
        // only once per nodegroup
        if (node.getNodeGroupOrdinal() != 0)
        {
            return true;
        }
        NodeGroup nodeGroup = node.getNodeGroup();
        Path targetDir = nodeGroup.getLocalArtifactPath().resolve("k8s_logs");
        FileUtils.createDirs(targetDir);
        boolean ok = node.getProvider(KubeControlProvider.class).inNamespace(Optional.empty(),
            namespacedKubeCtl -> namespacedKubeCtl
                .execute("cluster-info dump -A --output-directory=" + targetDir)
                .waitForSuccess());
        if (!ok)
        {
            return false;
        }
        Optional<String> namespace = containerLogsNamespaceSpec.optionalValue(nodeGroup);
        if (!namespace.isPresent())
        {
            return true;
        }

        final boolean ignoreFailures = ignoreFailuresSpec.value(nodeGroup);
        final boolean capturePreviousContainerLogs = capturePreviousContainerLogsSpec.value(nodeGroup);
        final LogFailure logFailure = ignoreFailures ? logger()::info : logger()::error;

        return node.getProvider(KubeControlProvider.class).inNamespace(namespace,
            namespacedKubeCtl -> {
                List<String> podNames = namespacedKubeCtl.findPodNames();
                boolean success = true;
                for (String podName : podNames)
                {
                    try
                    {
                        List<String> allContainersInPod = namespacedKubeCtl.getAllContainersInPod(podName);
                        for (String container : allContainersInPod)
                        {
                            try
                            {
                                namespacedKubeCtl.captureContainerLogs(podName, Optional.of(container),
                                    targetDir.resolve("pod-" + podName + "-" + container + ".logs"),
                                    capturePreviousContainerLogs);
                            }
                            catch (RuntimeException err)
                            {
                                logFailure.log("Cannot get logs for pod {} container {}", podName, container, err);
                                success = false;
                            }
                        }
                    }
                    catch (RuntimeException err)
                    {
                        logFailure.log("Cannot get logs for pod {}", podName, err);
                        success = false;
                    }
                }
                return success || ignoreFailures;
            });
    }
}
