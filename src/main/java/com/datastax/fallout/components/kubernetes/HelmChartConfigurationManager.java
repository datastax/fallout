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

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.common.spec.GitClone;
import com.datastax.fallout.components.common.spec.KubernetesDeploymentManifestSpec;
import com.datastax.fallout.ops.ConfigurationManager;
import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.util.Duration;

import static com.datastax.fallout.ops.FileSpec.GitFileSpec;

@AutoService(ConfigurationManager.class)
public class HelmChartConfigurationManager extends ConfigurationManager
{
    private final String prefix = "fallout.configuration.management.k8s.helm.";
    private final String name = "helm";
    private final String description = "Installs and manages a specified helm chart";

    private final PropertySpec<String> helmChartTypeSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .name("helm.install.type")
        .defaultOf("repo")
        .options("repo", "git")
        .description("Specify if the chart should be installed from a helm repo or from a private git repo")
        .build();

    private final PropertySpec<String> helmInstalledNameSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .name("helm.install.name")
        .description("The unique name of the helm service to be referenced in other charts")
        .required()
        .build();

    private final PropertySpec<String> helmRepoNameSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .dependsOn(helmChartTypeSpec, "repo")
        .name("helm.repo.name")
        .description("Name of the helm repo: e.g. bitnami")
        .build();

    private final PropertySpec<String> helmRepoUrlSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .dependsOn(helmChartTypeSpec, "repo")
        .name("helm.repo.url")
        .description("Url of the helm repo: e.g. https://charts.bitnami.com/bitnami")
        .build();

    private final PropertySpec<String> helmChartNameSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .dependsOn(helmChartTypeSpec, "repo")
        .description("Name of the chart to install from the helm repo: e.g. etcd")
        .name("helm.chart.name")
        .build();

    private final GitClone gitClone = new GitClone(this::prefix, "", null, "master", helmChartTypeSpec, "git");

    private final PropertySpec<String> chartLocationInRepoSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .dependsOn(helmChartTypeSpec, "git")
        .description("The relative location of the helm chart found")
        .name("git.chart.location")
        .build();

    private final PropertySpec<String> namespaceSpec =
        KubernetesDeploymentManifestSpec.buildNameSpaceSpec(this::prefix);

    private final PropertySpec<FileProvider.LocalManagedFileRef> helmInstallOptionsSpec =
        buildHelmValuesFileSpec(this::prefix);

    private final PropertySpec<String> providerClassSpec = PropertySpecBuilder.createStr(prefix)
        .runtimePrefix(this::prefix)
        .category("provider")
        .name("provider.class")
        .description("Simple class name of the fallout provider this helm chart adds")
        .suggestions("CassandraContactPointProvider")
        .build();

    private final PropertySpec<List<String>> providerArgsSpec = PropertySpecBuilder.createStrList(prefix)
        .runtimePrefix(this::prefix)
        .category("provider")
        .name("provider.args")
        .description("Options to be passed to the specified provider class constructor")
        .suggestions(ImmutableList.of("9042"))
        .build();

    private final PropertySpec<String> chartVersionSpec = buildHelmChartVersionSpec(this::prefix);
    private final PropertySpec<Duration> installTimeoutSpec = buildHelmInstallTimeoutSpec(this::prefix);
    private final PropertySpec<Boolean> installDebugSpec = buildHelmInstallDebugSpec(this::prefix);

    String installName;
    String chartLocation;

    @Override
    public String prefix()
    {
        //Let's this CM be used multiple times in the same nodeGroup
        return getInstanceName() != null ? prefix + getInstanceName() + "." : prefix;
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
        return ImmutableList.<PropertySpec<?>>builder()
            .add(helmChartTypeSpec)
            .add(helmInstalledNameSpec)
            .add(installDebugSpec)
            .add(installTimeoutSpec)
            .add(helmRepoUrlSpec, helmRepoNameSpec, helmChartNameSpec)
            .add(chartLocationInRepoSpec)
            .addAll(gitClone.getSpecs())
            .add(namespaceSpec, helmInstallOptionsSpec)
            .add(providerClassSpec, providerArgsSpec)
            .add(chartVersionSpec)
            .build();
    }

    Path cloneDir()
    {
        return getNodeGroup().getLocalArtifactPath().resolve(name);
    }

    protected boolean inNamespace(Function<KubeControlProvider.NamespacedKubeCtl, Boolean> function)
    {
        return getNodeGroup().findFirstRequiredProvider(KubeControlProvider.class)
            .inNamespace(namespaceSpec.optionalValue(getNodeGroup()), function);
    }

    @Override
    public NodeGroup.State checkStateImpl(NodeGroup nodeGroup)
    {
        boolean deployed = inNamespace(
            namespacedKubeCtl -> namespacedKubeCtl.checkHelmChartDeployed(helmInstalledNameSpec.value(nodeGroup)));

        if (deployed)
        {
            //Might need to re-clone the helm repo if re-using the cluster
            if (setupParameters(nodeGroup))
            {
                // TODO: return CONFIGURED post FAL-970
                return NodeGroup.State.STARTED_SERVICES_RUNNING;
            }
        }

        return NodeGroup.State.STARTED_SERVICES_UNCONFIGURED;
    }

    public boolean setupParameters(NodeGroup nodeGroup)
    {
        installName = helmInstalledNameSpec.value(nodeGroup);

        if (helmChartTypeSpec.value(nodeGroup).equals("git"))
        {
            if (!cloneDir().toFile().exists())
            {
                boolean helmChartCloned = nodeGroup.createLocalFile(cloneDir(),
                    GitFileSpec.create(gitClone.getRepo(nodeGroup), gitClone.getBranch(nodeGroup)));

                if (!helmChartCloned)
                {
                    logger().error("Could not clone {} helm chart", name);
                    return false;
                }
            }

            chartLocation = cloneDir().resolve(chartLocationInRepoSpec.value(nodeGroup)).toString();
        }
        else
        {
            String repoName = helmRepoNameSpec.value(nodeGroup);
            String chartName = helmChartNameSpec.value(nodeGroup);
            String repoUrl = helmRepoUrlSpec.value(nodeGroup);

            if (repoName != null)
            {
                if (!inNamespace(namespacedKubeCtl -> namespacedKubeCtl.addHelmRepo(repoName, repoUrl)))
                {
                    logger().error("Could not add helm repo {}", repoName);
                    return false;
                }

                //Append repo name if missing
                if (!chartName.contains("/"))
                    chartName = repoName + "/" + chartName;
            }

            chartLocation = chartName;
        }

        return true;
    }

    @Override
    public boolean configureImpl(NodeGroup nodeGroup)
    {
        if (checkStateImpl(nodeGroup) == NodeGroup.State.STARTED_SERVICES_CONFIGURED)
            return true;

        if (setupParameters(nodeGroup))
        {
            return inNamespace(namespacedKubeCtl -> namespacedKubeCtl.installHelmChart(installName, chartLocation,
                Map.of(), getOptionsFile(), installDebugSpec.value(nodeGroup), installTimeoutSpec.value(nodeGroup),
                chartVersionSpec.optionalValue(nodeGroup)));
        }
        return false;
    }

    private Optional<Path> getOptionsFile()
    {
        return helmInstallOptionsSpec.optionalValue(getNodeGroup()).map(lmf -> lmf.fullPath(getNodeGroup()));
    }

    @Override
    public boolean unconfigureImpl(NodeGroup nodeGroup)
    {
        return inNamespace(
            namespacedKubeCtl -> namespacedKubeCtl.uninstallHelmChart(helmInstalledNameSpec.value(nodeGroup)));
    }

    @Override
    public boolean registerProviders(Node node)
    {
        //First setup the helm upgrade provider
        try
        {
            new HelmProvider(node, installName, chartLocation, getOptionsFile());
        }
        catch (Throwable t)
        {
            logger().error("Error setting up helm provider {} {} ", installName, chartLocation, t);
            return false;
        }

        //Add the provider added by the chart if defined
        String providerClassName = providerClassSpec.value(node);
        List<String> providerArgs = providerArgsSpec.value(node);

        if (providerClassName != null)
        {
            Class clazz;
            try
            {
                clazz = this.getClass().getClassLoader().loadClass(providerClassName);
            }
            catch (ClassNotFoundException e)
            {
                logger().error("Unable to load provider class: {}", providerClassName);
                return false;
            }

            if (!Provider.class.isAssignableFrom(clazz))
            {
                logger().error("Class {} is found but not a fallout provider class", providerClassName);
                return false;
            }

            int numArgs = providerArgs == null ? 0 : providerArgs.size();

            try
            {
                Class argTypes[] = new Class[numArgs + 1];
                Object args[] = new Object[argTypes.length];

                argTypes[0] = Node.class;
                args[0] = node;

                for (int i = 1; i < argTypes.length; i++)
                {
                    argTypes[i] = String.class;
                    args[i] = providerArgs.get(i - 1);
                }

                clazz.getConstructor(argTypes).newInstance(args);
            }
            catch (NoSuchMethodException e)
            {
                logger().error("No Provider class {} constructor with {} String args found", providerClassName,
                    providerArgs == null ? 0 : providerArgs.size());
                return false;
            }
            catch (IllegalAccessException | InstantiationException | InvocationTargetException e)
            {
                logger().error("Error encountered when creating provider class {}", providerClassName, e);
                return false;
            }
        }

        return true;
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
    {
        String providerClassName = providerClassSpec.value(nodeGroupProperties);
        List<String> providerArgs = providerArgsSpec.value(nodeGroupProperties);

        if (providerClassName != null)
        {
            Class clazz;
            try
            {
                clazz = this.getClass().getClassLoader().loadClass(providerClassName);
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Unable to load specified provider class: " + providerClassName);
            }

            if (!Provider.class.isAssignableFrom(clazz))
            {
                throw new RuntimeException(
                    "Specified provider class is found but not a fallout provider class: " + providerClassName);
            }

            int numArgs = providerArgs == null ? 0 : providerArgs.size();

            try
            {
                Class argTypes[] = new Class[numArgs + 1];
                Arrays.fill(argTypes, String.class);
                argTypes[0] = Node.class;

                clazz.getConstructor(argTypes);

                return Set.of(clazz, HelmProvider.class);
            }
            catch (NoSuchMethodException e)
            {
                throw new RuntimeException("Specified provider class has no constructor with " + numArgs +
                    " String arguments: " + providerClassName);
            }
        }

        return Set.of(HelmProvider.class);
    }

    public static PropertySpec<FileProvider.LocalManagedFileRef> buildHelmValuesFileSpec(String prefix)
    {
        return buildHelmValuesFileSpec(() -> prefix);
    }

    public static PropertySpec<FileProvider.LocalManagedFileRef> buildHelmValuesFileSpec(Supplier<String> prefix)
    {
        return PropertySpecBuilder.createLocalManagedFileRef(prefix.get())
            .name("helm.install.values.file")
            .runtimePrefix(prefix)
            .description("Yaml file of options to pass to helm install")
            .build();
    }

    public static PropertySpec<String> buildHelmChartVersionSpec(Supplier<String> prefix)
    {
        return PropertySpecBuilder.createStr(prefix.get())
            .runtimePrefix(prefix)
            .name("helm.chart.version")
            .description("Version of the helm chart to install. Given to the --version argument.")
            .build();
    }

    public static PropertySpec<Duration> buildHelmInstallTimeoutSpec(Supplier<String> prefix)
    {
        return PropertySpecBuilder.createDuration(prefix.get())
            .runtimePrefix(prefix)
            .name("helm.install.timeout")
            .description(
                "Maximum amount of time to wait for Helm install. Value is given to helm's --timeout parameter.")
            .defaultOf(Duration.minutes(5))
            .build();
    }

    public static PropertySpec<Boolean> buildHelmInstallDebugSpec(Supplier<String> prefix)
    {
        return PropertySpecBuilder.createBool(prefix.get())
            .runtimePrefix(prefix)
            .name("helm.install.debug")
            .description("Debug 'helm install' command execution")
            .defaultOf(false)
            .build();
    }
}
