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
import java.util.Optional;
import java.util.function.Function;

import com.datastax.fallout.components.kubernetes.KubeControlProvider.HelmInstallValues;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.util.Duration;

public class HelmProvider extends Provider
{
    private final String installName;
    private final String chartLocation;
    private final List<String> originalValuesFiles;

    public HelmProvider(Node node, String installName, String chartLocation, List<String> originalValuesFiles)
    {
        super(node, false);
        this.installName = installName;
        this.chartLocation = chartLocation;
        this.originalValuesFiles = originalValuesFiles;

        register();
    }

    @Override
    public String name()
    {
        return installName;
    }

    public boolean upgrade(Optional<String> namespace, List<String> setValues, boolean debug, Duration timeout,
        Optional<String> version)
    {
        return inNamespace(namespace, kubectl -> kubectl.upgradeHelmChart(
            installName, chartLocation, HelmInstallValues.of(originalValuesFiles, setValues),
            debug, timeout, version));
    }

    private boolean inNamespace(Optional<String> namespace,
        Function<KubeControlProvider.NamespacedKubeCtl, Boolean> function)
    {
        return node().getProvider(KubeControlProvider.class).inNamespace(namespace, function);
    }
}
