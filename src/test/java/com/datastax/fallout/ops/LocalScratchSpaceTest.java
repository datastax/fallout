/*
 * Copyright 2020 DataStax, Inc.
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.Workload;
import com.datastax.fallout.ops.configmanagement.FakeConfigurationManager;
import com.datastax.fallout.ops.provisioner.FakeProvisioner;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class LocalScratchSpaceTest extends EnsembleFalloutTest
{
    protected ActiveTestRun activeTestRun;

    @Before
    public void setup()
    {
        NodeGroupBuilder ngBuilder = NodeGroupBuilder.create()
            .withName("temp-files-test")
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(new FakeConfigurationManager())
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1)
            .withLoggers(new JobConsoleLoggers())
            .withTestRunArtifactPath(persistentTestOutputDir());

        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withObserverGroup(ngBuilder)
            .withControllerGroup(ngBuilder)
            .withServerGroup(ngBuilder)
            .withClientGroup(ngBuilder);

        activeTestRun = createActiveTestRunBuilder()
            .withEnsembleBuilder(ensembleBuilder, true)
            .withWorkload(new Workload(new ArrayList<>(), new HashMap<>(), new HashMap<>()))
            .build();
    }

    private Set<Path> created = new HashSet<>();

    abstract LocalScratchSpace getLocalScratchSpace();

    abstract Runnable closeLocalScratchSpaceOwner();

    @Test
    public void all_temp_files_and_directories_are_deleted_on_close()
    {
        LocalScratchSpace localScratchSpace = getLocalScratchSpace();

        created.add(localScratchSpace.createFile("file1", ".txt"));
        created.add(localScratchSpace.createFile("file2", ".log"));
        created.add(localScratchSpace.createDirectory("dir1"));
        created.add(localScratchSpace.createDirectory("dir2"));
        created.forEach(path -> assertThat(path).exists());

        closeLocalScratchSpaceOwner().run();

        created.forEach(path -> assertThat(path).doesNotExist());
    }

    public static class NodeGroupLocalScratchSpaceTest extends LocalScratchSpaceTest
    {
        LocalScratchSpace getLocalScratchSpace()
        {
            return activeTestRun.getEnsemble().getAllNodeGroups().get(0).getLocalScratchSpace();
        }

        Runnable closeLocalScratchSpaceOwner()
        {
            return activeTestRun.getEnsemble().getAllNodeGroups().get(0)::close;
        }
    }

    public static class EnsembleLocalScratchSpaceTest extends LocalScratchSpaceTest
    {
        LocalScratchSpace getLocalScratchSpace()
        {
            return activeTestRun.getEnsemble().getLocalScratchSpace();
        }

        Runnable closeLocalScratchSpaceOwner()
        {
            return activeTestRun.getEnsemble()::close;
        }
    }

    public static class ActiveTestRunLocalScratchSpaceTest extends LocalScratchSpaceTest
    {
        @Override
        LocalScratchSpace getLocalScratchSpace()
        {
            return activeTestRun.getEnsemble().getLocalScratchSpace();
        }

        @Override
        Runnable closeLocalScratchSpaceOwner()
        {
            return activeTestRun::close;
        }
    }
}
