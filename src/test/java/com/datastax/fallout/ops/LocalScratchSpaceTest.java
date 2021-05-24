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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.components.impl.FakeModule;
import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.Workload;
import com.datastax.fallout.ops.TestRunScratchSpaceFactory.LocalScratchSpace;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;

public abstract class LocalScratchSpaceTest extends EnsembleFalloutTest<FalloutConfiguration>
{
    protected ActiveTestRun activeTestRun;

    @BeforeEach
    public void setup()
    {
        NodeGroupBuilder ngBuilder = NodeGroupBuilder.create()
            .withName("temp-files-test")
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(new FakeConfigurationManager())
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1)
            .withTestRunArtifactPath(persistentTestOutputDir());

        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withObserverGroup(ngBuilder)
            .withControllerGroup(ngBuilder)
            .withServerGroup(ngBuilder)
            .withClientGroup(ngBuilder);

        activeTestRun = createActiveTestRunBuilder()
            .withEnsembleBuilder(ensembleBuilder)
            .withWorkload(new Workload(new ArrayList<>(), new HashMap<>(), new HashMap<>()))
            .build();
    }

    private Set<Path> created = new HashSet<>();

    abstract LocalScratchSpace getLocalScratchSpace();

    @Test
    public void all_temp_files_and_directories_are_deleted_on_close() throws IOException
    {
        LocalScratchSpace localScratchSpace = getLocalScratchSpace();

        created.add(Files.createFile(localScratchSpace.resolve("file1.txt")));
        created.add(Files.createFile(localScratchSpace.resolve("file2.log")));
        created.add(localScratchSpace.makeScratchSpaceFor("dir1").getPath());
        created.add(localScratchSpace.makeScratchSpaceFor("dir2").getPath());
        created.forEach(path -> assertThat(path).exists());

        activeTestRun.close();

        created.forEach(path -> assertThat(path).doesNotExist());
    }

    public static class NodeGroupLocalScratchSpaceTest extends LocalScratchSpaceTest
    {
        LocalScratchSpace getLocalScratchSpace()
        {
            return activeTestRun.getEnsemble().getAllNodeGroups().get(0).getLocalScratchSpace();
        }
    }

    public static class WorkloadLocalScratchSpaceTest extends LocalScratchSpaceTest
    {
        LocalScratchSpace getLocalScratchSpace()
        {
            return activeTestRun.getEnsemble().makeScratchSpaceFor(new FakeModule());
        }
    }
}
