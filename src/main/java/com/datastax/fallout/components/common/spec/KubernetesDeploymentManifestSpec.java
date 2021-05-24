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
package com.datastax.fallout.components.common.spec;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.kubernetes.KubernetesManifestSpec;
import com.datastax.fallout.exceptions.InvalidConfigurationException;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.util.Duration;

/**
 * Container for the PropertySpecs required for the KubernetesManifestConfigurationManager to deploy a Kubernetes
 * manifest.
 */
public class KubernetesDeploymentManifestSpec extends KubernetesManifestSpec
{
    private final PropertySpec<String> nameSpaceSpec;

    private final ManifestWaitOptions waitOptions;

    private final PropertySpec<ManifestWaitStrategy> waitStrategySpec;
    private final PropertySpec<String> waitConditionSpec;
    private final PropertySpec<Duration> waitTimeoutSpec;
    private final PropertySpec<String> podLabelSpec;
    private final PropertySpec<String> containerNameSpec;
    private final PropertySpec<Integer> expectedContainersSpec;
    private final PropertySpec<String> imageNameSpec;

    public enum ManifestWaitStrategy
    {
        FIXED_DURATION,
        WAIT_ON_MANIFEST,
        WAIT_ON_PODS,
        WAIT_ON_CONTAINERS,
        WAIT_ON_IMAGE
    }

    public KubernetesDeploymentManifestSpec(String prefix, String name, PropertySpec<String> sharedNameSpaceSpec,
        boolean required, ManifestWaitOptions waitOptions)
    {
        super(prefix, name, required);
        nameSpaceSpec = sharedNameSpaceSpec;

        this.waitOptions = waitOptions;

        waitStrategySpec = null;
        waitConditionSpec = null;
        waitTimeoutSpec = null;
        podLabelSpec = null;
        expectedContainersSpec = null;
        containerNameSpec = null;
        imageNameSpec = null;
    }

    public KubernetesDeploymentManifestSpec(String prefix)
    {
        super(prefix);
        nameSpaceSpec = buildNameSpaceSpec(prefix);
        waitStrategySpec = buildWaitStrategySpec(prefix, Optional.empty(), true);
        waitConditionSpec = buildWaitConditionSpec(prefix, Optional.empty());
        waitTimeoutSpec = buildWaitTimeoutSpec(prefix, Optional.empty());
        podLabelSpec = buildPodLabelSpec(prefix, Optional.empty(), waitStrategySpec);
        containerNameSpec = buildContainerNameSpec(prefix, Optional.empty(), waitStrategySpec);
        expectedContainersSpec = buildExpectedContainersSpec(prefix, Optional.empty(), waitStrategySpec);
        imageNameSpec = buildImageNameSpec(prefix, Optional.empty(), waitStrategySpec);

        waitOptions = null;
    }

    public List<PropertySpec<?>> getPropertySpecs()
    {
        ImmutableList.Builder<PropertySpec<?>> builder = ImmutableList.<PropertySpec<?>>builder();
        if (waitOptions == null)
        {
            builder.add(nameSpaceSpec);
        }
        builder.addAll(super.getPropertySpecs());
        if (waitOptions == null)
        {
            builder.add(waitStrategySpec, waitConditionSpec, waitTimeoutSpec, podLabelSpec, containerNameSpec,
                expectedContainersSpec);
        }
        return builder.build();
    }

    public void validateProperties(PropertyGroup properties)
    {
        if (waitOptions != null)
        {
            return;
        }

        ManifestWaitStrategy strategy = waitStrategySpec.value(properties);
        checkSpecIsPresent(waitTimeoutSpec, properties);
        switch (strategy)
        {
            case WAIT_ON_PODS:
                checkSpecIsPresent(podLabelSpec, properties);
                // FALL THROUGH
            case WAIT_ON_MANIFEST:
            case WAIT_ON_IMAGE:
                checkSpecIsPresent(waitConditionSpec, properties);
                break;
            case WAIT_ON_CONTAINERS:
                checkSpecIsPresent(containerNameSpec, properties);
                checkSpecIsPresent(expectedContainersSpec, properties);
                break;
            default:
                break;
        }
    }

    private void checkSpecIsPresent(PropertySpec<?> spec, PropertyGroup properties)
    {
        if (spec.value(properties) == null)
        {
            throw new PropertySpec.ValidationException(String.format("Missing required property: %s", spec.name()));
        }
    }

    public Optional<String> maybeGetNameSpace(PropertyGroup properties)
    {
        return nameSpaceSpec.optionalValue(properties);
    }

    public static class ManifestWaitOptions
    {
        private final ManifestWaitStrategy strategy;
        private final Duration timeout;
        private final String condition;
        private final String podLabel;
        private final String containerName;
        private final Function<String, Integer> expectedContainers;
        private final Function<String, String> imageName;

        public ManifestWaitOptions(ManifestWaitStrategy strategy, Duration timeout, String condition, String podLabel,
            String containerName, Function<String, Integer> expectedContainers, Function<String, String> imageName)
        {
            this.strategy = strategy;
            this.timeout = timeout;
            this.condition = condition;
            this.podLabel = podLabel;
            this.containerName = containerName;
            this.expectedContainers = expectedContainers;
            this.imageName = imageName;
        }

        public static ManifestWaitOptions fixedDuration(Duration timeout)
        {
            return new ManifestWaitOptions(ManifestWaitStrategy.FIXED_DURATION, timeout, null, null, null, null, null);
        }

        private static ManifestWaitOptions manifest(Duration timeout, String condition)
        {
            return new ManifestWaitOptions(ManifestWaitStrategy.WAIT_ON_MANIFEST, timeout, condition,
                null, null, null, null);
        }

        private static ManifestWaitOptions pods(Duration timeout, String condition, String podLabel)
        {
            return new ManifestWaitOptions(ManifestWaitStrategy.WAIT_ON_PODS, timeout, condition, podLabel, null, null,
                null);
        }

        public static ManifestWaitOptions containers(Duration timeout, String containerName,
            Function<String, Integer> expectedContainers)
        {
            return new ManifestWaitOptions(ManifestWaitStrategy.WAIT_ON_CONTAINERS, timeout, null, null, containerName,
                expectedContainers, null);
        }

        public static ManifestWaitOptions image(Duration timeout, String condition, Function<String, String> imageName)
        {
            return new ManifestWaitOptions(ManifestWaitStrategy.WAIT_ON_IMAGE, timeout, condition, null, null, null,
                imageName);
        }

        public ManifestWaitStrategy getStrategy()
        {
            return strategy;
        }

        public String getCondition()
        {
            return condition;
        }

        public String getPodLabel()
        {
            return podLabel;
        }

        public Duration getTimeout()
        {
            return timeout;
        }

        public String getContainerName()
        {
            return containerName;
        }

        public int getExpectedContainers(String manifest)
        {
            return expectedContainers.apply(manifest);
        }

        public String getImageName(String manifest)
        {
            return imageName.apply(manifest);
        }
    }

    public ManifestWaitOptions getManifestWaitOptions(PropertyGroup properties)
    {
        if (waitOptions != null)
        {
            return waitOptions;
        }

        String condition = waitConditionSpec.value(properties);
        Duration timeout = waitTimeoutSpec.value(properties);
        switch (waitStrategySpec.value(properties))
        {
            case FIXED_DURATION:
                return ManifestWaitOptions.fixedDuration(timeout);
            case WAIT_ON_MANIFEST:
                return ManifestWaitOptions.manifest(timeout, condition);
            case WAIT_ON_PODS:
                return ManifestWaitOptions.pods(timeout, condition, podLabelSpec.value(properties));
            case WAIT_ON_CONTAINERS:
                return ManifestWaitOptions.containers(timeout, containerNameSpec.value(properties),
                    ignored -> expectedContainersSpec.value(properties));
            case WAIT_ON_IMAGE:
                return ManifestWaitOptions.image(timeout, condition, ignored -> imageNameSpec.value(properties));
            default:
                throw new InvalidConfigurationException("Invalid choice of Manifest Wait Strategy");
        }
    }

    public static PropertySpec<String> buildNameSpaceSpec(String prefix)
    {
        return nameSpaceSpecBuilder(prefix).build();
    }

    public static PropertySpec<String> buildNameSpaceSpec(Supplier<String> prefix)
    {
        return nameSpaceSpecBuilder(prefix.get()).runtimePrefix(prefix).build();
    }

    public static <T> PropertySpec<String> buildNameSpaceSpec(String prefix, PropertySpec<T> parent, T parentValue)
    {
        return nameSpaceSpecBuilder(prefix)
            .dependsOn(parent, parentValue)
            .build();
    }

    private static PropertySpecBuilder<String> nameSpaceSpecBuilder(String prefix)
    {
        return PropertySpecBuilder.createStr(prefix)
            .name("namespace")
            .description("Namespace to execute kubectl commands in.");
    }

    private static PropertySpec<ManifestWaitStrategy> buildWaitStrategySpec(String prefix, Optional<String> name,
        boolean required)
    {
        return PropertySpecBuilder.createEnum(prefix, ManifestWaitStrategy.class)
            .name(SpecUtils.buildFullName(name, "wait.strategy"))
            .description(
                "Method of waiting for resources / deployment to be ready.  FIXED_DURATION will just pause until wait.timeout has passed; WAIT_ON_CONTAINERS will wait for wait.expected_containers of wait.container_name to appear with a timeout of wait.timeout; WAIT_ON_PODS will wait for the pods selected by wait.pod_label to satisfy wait.condition; WAIT_ON_IMAGE will wait for the pods running the image from the manifest to satisfy wait.condition; and WAIT_ON_MANIFEST will run kubectl wait --for=<wait.condition> --timeout=<wait.timeout> -f <fallout-internal-manifest-path>")
            .required(required)
            .build();
    }

    private static PropertySpec<String> buildWaitConditionSpec(String prefix, Optional<String> name)
    {
        return PropertySpecBuilder.createStr(prefix)
            .name(SpecUtils.buildFullName(name, "wait.condition"))
            .description(
                "Condition to wait for (e.g. condition=Ready); required for WAIT_ON_MANIFEST, WAIT_ON_PODS and WAIT_ON_IMAGE")
            .build();
    }

    private static PropertySpec<Duration> buildWaitTimeoutSpec(String prefix, Optional<String> name)
    {
        return PropertySpecBuilder.createDuration(prefix)
            .name(SpecUtils.buildFullName(name, "wait.timeout"))
            .description("Duration to wait for condition to be met.")
            .defaultOf(Duration.minutes(15))
            .build();
    }

    private static PropertySpec<String> buildPodLabelSpec(String prefix, Optional<String> name,
        PropertySpec<ManifestWaitStrategy> parent)
    {
        return PropertySpecBuilder.createStr(prefix)
            .name(SpecUtils.buildFullName(name, "wait.pod_label"))
            .description("Label selector, to identify the pod(s) to wait on.")
            .dependsOn(parent, ManifestWaitStrategy.WAIT_ON_PODS)
            .build();
    }

    private static PropertySpec<String> buildImageNameSpec(String prefix, Optional<String> name,
        PropertySpec<ManifestWaitStrategy> parent)
    {
        return PropertySpecBuilder.createStr(prefix)
            .name(SpecUtils.buildFullName(name, "wait.image_name"))
            .description("Name of an image running in a container to identify the pod to wait on.")
            .dependsOn(parent, ManifestWaitStrategy.WAIT_ON_IMAGE)
            .build();
    }

    private PropertySpec<String> buildContainerNameSpec(String prefix, Optional<String> name,
        PropertySpec<ManifestWaitStrategy> parent)
    {
        return PropertySpecBuilder.createStr(prefix)
            .name(SpecUtils.buildFullName(name, "wait.container_name"))
            .description("Name of container to wait for.")
            .dependsOn(parent, ManifestWaitStrategy.WAIT_ON_CONTAINERS)
            .build();
    }

    private PropertySpec<Integer> buildExpectedContainersSpec(String prefix, Optional<String> name,
        PropertySpec<ManifestWaitStrategy> parent)
    {
        return PropertySpecBuilder.createInt(prefix)
            .name(SpecUtils.buildFullName(name, "wait.expected_containers"))
            .description("Number of containers to wait for.")
            .dependsOn(parent, ManifestWaitStrategy.WAIT_ON_CONTAINERS)
            .build();
    }
}
