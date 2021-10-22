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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.datastax.fallout.components.common.provider.FileProvider;
import com.datastax.fallout.components.common.spec.SpecUtils;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;

import static com.datastax.fallout.util.MustacheFactoryWithoutHTMLEscaping.renderWithScopes;

/**
 * Container for the PropertySpecs required to define the contents and location of a Kubernetes manifest.
 */
public class KubernetesManifestSpec
{
    private final PropertySpec<FileProvider.LocalManagedFileRef> manifestContentSpec;
    private final PropertySpec<Map<String, Object>> manifestContentTemplateSpec;

    public KubernetesManifestSpec(String prefix)
    {
        this(prefix, Optional.empty(), true);
    }

    public KubernetesManifestSpec(String prefix, String name, boolean required)
    {
        this(prefix, Optional.of(name), required);
    }

    private KubernetesManifestSpec(String prefix, Optional<String> name, boolean required)
    {
        manifestContentSpec = PropertySpecBuilder.createLocalManagedFileRef(prefix)
            .name(SpecUtils.buildFullName(name, "manifest"))
            .description("Entire manifest file to apply.  If this is a directory, then all the files in the " +
                "directory are joined together to make a single manifest.")
            .required(required)
            .build();

        manifestContentTemplateSpec = PropertySpecBuilder
            .createMap(prefix)
            .name(SpecUtils.buildFullName(name, "template_params"))
            .description("If set, then the manifest will be processed with Mustache using this map of template " +
                "parameters to values.")
            .build();
    }

    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(manifestContentSpec, manifestContentTemplateSpec);
    }

    public boolean isPresent(PropertyGroup properties)
    {
        return manifestContentSpec.optionalValue(properties).isPresent();
    }

    public PropertySpec<FileProvider.LocalManagedFileRef> getManifestContentSpec()
    {
        return manifestContentSpec;
    }

    /**
       This method reconciles different potential inputs to produce a single manifest file. In order to only do this once,
       the method must be synchronized.
       The potential inputs are as follows:
         - Given a directory, this method will join all yaml files within the directory into a single multi-doc yaml file.
         - If template params are present in the manifestContentTemplateSpec, they will be rendered using Mustache. This
           also applies to the multi-doc case.
         - If the manifest content is a single un-templated file, it will be used directly.
     */
    protected synchronized Path getManifestArtifactPath(NodeGroup nodeGroup, PropertyGroup properties)
    {
        Path managedSourceFile = manifestContentSpec.value(properties).fullPath(nodeGroup);
        Path deployedManifestArtifact = nodeGroup.getLocalArtifactPath().resolve(String.format(
            "%s-deployed-manifest.yaml", managedSourceFile.getFileName().toString().replace(".yaml", "")));
        if (deployedManifestArtifact.toFile().exists())
        {
            return deployedManifestArtifact;
        }

        Optional<String> manifestContent = Optional.empty();
        if (managedSourceFile.toFile().isDirectory())
        {
            manifestContent = Optional.of(joinYamlFilesInDirectory(managedSourceFile));
        }
        if (manifestContentTemplateSpec.optionalValue(properties).isPresent())
        {
            if (manifestContent.isEmpty())
            {
                manifestContent = Optional.of(FileUtils.readString(managedSourceFile));
            }
            manifestContent = Optional.of(
                renderWithScopes(manifestContent.get(),
                    List.of(manifestContentTemplateSpec.value(properties))));
        }
        return manifestContent
            .map(_manifestContent -> {
                FileUtils.writeString(deployedManifestArtifact, _manifestContent);
                return deployedManifestArtifact;
            })
            .orElse(managedSourceFile);
    }

    private static String joinYamlFilesInDirectory(Path parentDirectory)
    {
        List<Path> yamlsInDir = Exceptions.getUncheckedIO(() -> {
            try (Stream<Path> pathStream = Files.walk(parentDirectory, 1))
            {
                return pathStream
                    .filter(p -> !p.equals(parentDirectory))
                    .filter(p -> p.toString().endsWith(".yaml"))
                    .collect(Collectors.toList());
            }
        });

        return yamlsInDir.stream()
            .map(FileUtils::readString)
            .collect(Collectors.joining("\n---\n"));
    }

    protected String getManifestContent(NodeGroup nodeGroup)
    {
        return getManifestContent(nodeGroup, nodeGroup.getProperties());
    }

    protected String getManifestContent(NodeGroup nodeGroup, PropertyGroup properties)
    {
        return Exceptions.getUncheckedIO(() -> Files.readString(getManifestArtifactPath(nodeGroup, properties)));
    }
}
