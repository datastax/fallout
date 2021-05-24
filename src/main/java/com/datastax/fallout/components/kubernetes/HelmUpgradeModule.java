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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.auto.service.AutoService;
import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec;
import com.datastax.fallout.harness.Module;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.components.kubernetes.HelmChartConfigurationManager.buildHelmChartVersionSpec;
import static com.datastax.fallout.components.kubernetes.HelmChartConfigurationManager.buildHelmInstallDebugSpec;
import static com.datastax.fallout.components.kubernetes.HelmChartConfigurationManager.buildHelmInstallTimeoutSpec;

@AutoService(Module.class)
public class HelmUpgradeModule extends Module
{
    private final String prefix = "fallout.module.k8s.helm.";
    private final String name = "helm";
    private final String description = "Modifies a specified helm chart";

    private final PropertySpec<Duration> delaySpec = PropertySpecBuilder
        .createDuration(prefix)
        .name("delay")
        .description("How long to delay before running e.g. 10m, 13s")
        .build();

    private final PropertySpec<String> helmInstalledNameSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .name("helm.install.name")
        .description("The unique name of the helm that was added by the helm config manager")
        .required()
        .build();

    private final PropertySpec<Map<String, Object>> valueMapSpec = PropertySpecBuilder
        .<Object>createMap(prefix)
        .name("helm.install.values.yaml")
        .description("A Map of K V pairs that are to be set in the specified yaml.\n" +
            "Keys must be provided in the form of 'parent.child.subchild'")
        .required()
        .build();

    private final PropertySpec<Duration> installTimeoutSpec = buildHelmInstallTimeoutSpec(this::prefix);
    private final PropertySpec<Boolean> installDebugSpec = buildHelmInstallDebugSpec(this::prefix);

    private final PropertySpec<String> namespaceSpec =
        KubernetesDeploymentManifestSpec.buildNameSpaceSpec(this::prefix);
    private final PropertySpec<String> chartVersionSpec = buildHelmChartVersionSpec(this::prefix);

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
    public List<PropertySpec<?>> getModulePropertySpecs()
    {
        return List.of(helmInstalledNameSpec, valueMapSpec, namespaceSpec, delaySpec, installTimeoutSpec,
            installDebugSpec);

    }

    @Override
    public void run(Ensemble ensemble, PropertyGroup properties)
    {
        String installName = helmInstalledNameSpec.value(properties);
        Optional<HelmProvider> maybeHelm = ensemble.findProviderWithName(HelmProvider.class, installName);

        if (!maybeHelm.isPresent())
        {
            emitError("Missing helm for " + installName);
            return;
        }

        delaySpec.optionalValue(properties)
            .ifPresent(d -> {
                emitInfo("Sleeping " + d.toString());
                Uninterruptibles.sleepUninterruptibly(d.value, d.unit);
            });

        HelmProvider helm = maybeHelm.get();

        //Convert Map<String,Object> to Map<String, String>
        Map<String, String> values = valueMapSpec.value(properties)
            .entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));

        if (helm.upgrade(namespaceSpec.optionalValue(properties), values, installDebugSpec.value(properties),
            installTimeoutSpec.value(properties), chartVersionSpec.optionalValue(properties)))
        {
            emitOk("Upgraded helm chart " + installName);
        }
        else
        {
            emitFail("Failed to upgrade helm chart " + installName);
        }
    }
}
