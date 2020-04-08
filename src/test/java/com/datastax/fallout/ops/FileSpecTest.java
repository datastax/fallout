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

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import com.datastax.fallout.harness.ActiveTestRun;
import com.datastax.fallout.harness.EnsembleFalloutTest;
import com.datastax.fallout.harness.Workload;
import com.datastax.fallout.ops.configmanagement.NoopConfigurationManager;
import com.datastax.fallout.ops.configmanagement.RemoteFilesConfigurationManager;
import com.datastax.fallout.ops.providers.FileProvider;
import com.datastax.fallout.ops.provisioner.LocalProvisioner;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.YamlUtils;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class FileSpecTest extends EnsembleFalloutTest
{
    private static final String FILE_CONTENT = "Content in a file!";
    private static final String PATH_IN_YAML = "some/sub/path/";

    @Test
    public void remote_files_are_created_and_contain_correct_contents()
    {
        WritablePropertyGroup propertyGroup = new WritablePropertyGroup();
        RemoteFilesConfigurationManager filesCM = new RemoteFilesConfigurationManager();
        propertyGroup.with(filesCM).put("files", ImmutableList.of(addFileSpecProperties(createBasePropertiesMap())));

        NodeGroup nodeGroup = NodeGroupBuilder.create()
            .withProvisioner(new LocalProvisioner())
            .withConfigurationManager(filesCM)
            .withPropertyGroup(propertyGroup)
            .withNodeCount(1)
            .withName("files-configuration-test-group")
            .withLoggers(new JobConsoleLoggers())
            .withTestRunArtifactPath(persistentTestOutputDir())
            .build();

        assertThat(filesCM.configureImpl(nodeGroup)).isTrue();
        nodeGroup.getNodes().forEach(filesCM::registerProviders);

        FileProvider fileProvider = nodeGroup.findFirstRequiredProvider(FileProvider.RemoteFileProvider.class);
        assertFileCreatedAndContainsContent(fileProvider);

        assertThat(filesCM.unconfigureImpl(nodeGroup)).isTrue();
        assertFileRemoved(fileProvider);
    }

    @Test
    public void local_files_are_created_and_contain_correct_contents()
    {
        LocalFilesHandler localFilesHandler =
            new LocalFilesHandler(List.of(addFileSpecProperties(createBasePropertiesMap())));

        NodeGroupBuilder serverGroup = NodeGroupBuilder.create()
            .withName("server")
            .withProvisioner(new LocalProvisioner())
            .withConfigurationManager(new NoopConfigurationManager())
            .withPropertyGroup(new WritablePropertyGroup())
            .withLocalFilesHandler(localFilesHandler)
            .withNodeCount(1);

        NodeGroupBuilder otherGroups = NodeGroupBuilder.create()
            .withName("other")
            .withProvisioner(new LocalProvisioner())
            .withConfigurationManager(new NoopConfigurationManager())
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1);

        EnsembleBuilder ensembleBuilder = EnsembleBuilder.create()
            .withServerGroup(serverGroup)
            .withClientGroup(otherGroups)
            .withDefaultObserverGroup(otherGroups)
            .withControllerGroup(otherGroups);

        ActiveTestRun activeTestRun = createActiveTestRunBuilder()
            .withEnsembleBuilder(ensembleBuilder, true)
            .withWorkload(new Workload(new ArrayList<>(), new HashMap<>(), new HashMap<>()))
            .build();

        performTestRun(activeTestRun);
        FileProvider.LocalFileProvider fileProvider = activeTestRun.getEnsemble().getServerGroup("server")
            .findFirstRequiredProvider(FileProvider.LocalFileProvider.class);
        assertFileCreatedAndContainsContent(fileProvider);
    }

    abstract Map<String, Object> addFileSpecProperties(Map<String, Object> parentProperties);

    private Map<String, Object> createBasePropertiesMap()
    {
        Map<String, Object> basePropertiesMap = new HashMap<>();
        basePropertiesMap.put("path", getFilePathInYaml());
        return basePropertiesMap;
    }

    protected String getFilePathInYaml()
    {
        return PATH_IN_YAML + "file.txt";
    }

    private String getWrappedFilePathInYaml()
    {
        return String.format("<<file:%s>>", getFilePathInYaml());
    }

    protected void assertFileCreatedAndContainsContent(FileProvider fileProvider)
    {
        Path createdFile = fileProvider.getFullPath(getWrappedFilePathInYaml());
        assertThat(createdFile).exists();
        assertThat(createdFile).hasContent(getExpectedContent());
    }

    protected void assertFileRemoved(FileProvider fileProvider)
    {
        Path createdFile = fileProvider.getFullPath(getWrappedFilePathInYaml());
        assertThat(createdFile).doesNotExist();
    }

    protected String getExpectedContent()
    {
        throw new RuntimeException("getExpectedContent should be overridden in sub-classes or not used in assertions.");
    }

    public static class DataFileTest extends FileSpecTest
    {
        @Override
        Map<String, Object> addFileSpecProperties(Map<String, Object> parentProperties)
        {
            parentProperties.put("data", FILE_CONTENT);
            return parentProperties;
        }

        protected String getExpectedContent()
        {
            return FILE_CONTENT;
        }
    }

    public static class YamlFileTest extends FileSpecTest
    {
        @Override
        Map<String, Object> addFileSpecProperties(Map<String, Object> parentProperties)
        {
            parentProperties.put("yaml", fileContentMap());
            return parentProperties;
        }

        protected String getExpectedContent()
        {
            return YamlUtils.dumpYaml(fileContentMap());
        }
    }

    public static class JsonFileTest extends FileSpecTest
    {
        @Override
        Map<String, Object> addFileSpecProperties(Map<String, Object> parentProperties)
        {
            parentProperties.put("json", fileContentMap());
            return parentProperties;
        }

        protected String getExpectedContent()
        {
            return Utils.json(fileContentMap());
        }
    }

    private static Map<String, Object> fileContentMap()
    {
        Map<String, Object> fileContentMap = new HashMap<>(1);
        fileContentMap.put("file_content", FILE_CONTENT);
        return fileContentMap;
    }

    public static class URLFileTest extends FileSpecTest
    {
        @Override
        Map<String, Object> addFileSpecProperties(Map<String, Object> parentProperties)
        {
            parentProperties.put("url", "https://gist.githubusercontent.com/smccarthy788/" +
                "b4715de7e2c71e44cfd840a13fd73ee4/raw/de173e4b4ad4e6862340fd6189cd774b083542c7/files-cm-url.txt");
            return parentProperties;
        }

        @Override
        protected String getExpectedContent()
        {
            return FILE_CONTENT;
        }
    }

    public static class GitFileTest extends FileSpecTest
    {
        @Override
        Map<String, Object> addFileSpecProperties(Map<String, Object> parentProperties)
        {
            Map<String, String> gitPropsMap = new HashMap<>(2);
            gitPropsMap.put("repo", "git@github.com:datastax/fallout.git");
            gitPropsMap.put("branch", "master");
            parentProperties.put("git", gitPropsMap);
            return parentProperties;
        }

        @Override
        protected String getFilePathInYaml()
        {
            // checkout directly under root files directory
            return "";
        }

        @Override
        protected void assertFileCreatedAndContainsContent(FileProvider fileProvider)
        {
            Path readme = fileProvider.getFullPath("<<file:fallout/README.md>>");
            assertThat(readme).exists();
            BufferedReader reader =
                Exceptions.getUncheckedIO(() -> new BufferedReader(new FileReader(readme.toFile())));
            assertThat(reader.lines().collect(Collectors.joining())).contains("Fallout");
        }

        @Override
        protected void assertFileRemoved(FileProvider fileProvider)
        {
            Path createdFile = fileProvider.getFullPath("<<file:fallout/README.md>>");
            assertThat(createdFile).doesNotExist();
        }
    }
}
