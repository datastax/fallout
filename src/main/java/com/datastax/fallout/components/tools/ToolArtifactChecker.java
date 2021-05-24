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
package com.datastax.fallout.components.tools;

import java.nio.file.Path;
import java.util.List;

import com.google.auto.service.AutoService;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

@AutoService(ArtifactChecker.class)
public class ToolArtifactChecker extends ArtifactChecker implements ToolComponent
{
    private static final String NAME = "tool";
    private static final String PREFIX = "fallout.artifact_checkers." + NAME + ".";

    private static PropertySpec<String> toolSpec = PropertySpecBuilder.createStr(PREFIX)
        .name("tool")
        .description("The executable to run")
        .required()
        .build();

    private static PropertySpec<String> argsSpec = PropertySpecBuilder.createStr(PREFIX)
        .name("args")
        .description("Any extra arguments that should be passed to the executable")
        .build();

    private ToolExecutor toolExecutor;

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(toolSpec, argsSpec);
    }

    @Override
    public String prefix()
    {
        return PREFIX;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public String description()
    {
        return "Run the named tool on the artifacts; the tool will be run with " +
            "the artifact directory as the current directory";
    }

    @Override
    public void setToolExecutor(ToolExecutor toolExecutor)
    {
        this.toolExecutor = toolExecutor;
    }

    @Override
    public boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        return toolExecutor
            .executeTool(logger(),
                "artifact-checkers", rootArtifactLocation,
                toolSpec.value(this), argsSpec.optionalValue(this))
            .waitForSuccess();
    }
}
