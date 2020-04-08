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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.ops.impl.TestConfigurationManager;
import com.datastax.fallout.ops.provisioner.LocalProvisioner;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.ops.NodeGroup.State.STARTED_SERVICES_RUNNING;
import static com.datastax.fallout.ops.NodeGroupHelpers.assertSuccessfulTransition;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class FileTransferTest extends TestHelpers.FalloutTest
{
    private static final Logger logger = LoggerFactory.getLogger(FileTransferTest.class);

    private static final String TEMPFILE_CONTENTS = "testing data integrity";
    private static final String TEMPDIR_CONTENTS = "testdir data integrity";
    private static final String TEMPDEEPDIR_CONTENTS = "deepdir data integrity";
    private static final int NODE_COUNT = 3;

    private Path localRoot;

    private static NodeGroup testGroup;

    private static void startNodeGroup(String name, WritablePropertyGroup properties, Provisioner provisioner,
        ConfigurationManager configurationManager)
    {
        testGroup = NodeGroupBuilder
            .create()
            .withName(name)
            .withProvisioner(provisioner)
            .withConfigurationManager(configurationManager)
            .withPropertyGroup(properties)
            .withNodeCount(NODE_COUNT)
            .withLoggers(new JobConsoleLoggers())
            .withTestRunArtifactPath(persistentTestClassOutputDir().resolve("artifacts"))
            .build();

        assertSuccessfulTransition(testGroup, STARTED_SERVICES_RUNNING);
    }

    @AfterClass
    public static void stopNodeGroup()
    {
        if (testGroup != null)
        {
            assertSuccessfulTransition(testGroup, NodeGroup.State.DESTROYED);
        }
    }

    @Before
    public void setupRemoteFS() throws IOException
    {
        createRemoteFS();
        logger.info(String.format("Remote FS dir is %s", getRemoteRoot()));
    }

    protected void createRemoteFS() throws IOException
    {
        for (int i = 0; i != NODE_COUNT; ++i)
        {
            Files.createDirectories(Paths.get(remotePath("", i)));
        }
    }

    protected Path getRemoteRoot()
    {
        return persistentTestOutputDir().resolve("remote");
    }

    private String remotePath(String relativePath, int nodeOrdinal)
    {
        return remotePath(Paths.get(relativePath), nodeOrdinal);
    }

    protected String remotePath(Path relativePath, int nodeOrdinal)
    {
        return getRemoteRoot().resolve(String.format("node%d", nodeOrdinal)).resolve(relativePath).toString();
    }

    @Before
    public void createLocalFS() throws IOException
    {
        localRoot = persistentTestOutputDir().resolve("local");
        Files.createDirectories(localRoot);
        logger.info(String.format("Local FS dir is %s", localRoot));
    }

    private Path createLocalDir(String relativePath)
    {
        return createLocalDir(Paths.get(relativePath));
    }

    private Path createLocalDir(Path relativePath)
    {
        final Path dirPath = localRoot.resolve(relativePath);
        Exceptions.runUncheckedIO(() -> Files.createDirectories(dirPath));
        return dirPath;
    }

    private Path createLocalFile(Path relativePath, String contents)
    {
        return writeFile(createLocalDir(relativePath.getParent()).resolve(relativePath.getFileName()), contents);
    }

    protected void makeUnreadable(Node node, Path remotePath)
    {
        Exceptions.runUncheckedIO(() -> Files.setPosixFilePermissions(remotePath, Collections.emptySet()));
    }

    public static class WithLocalProvisioner extends FileTransferTest
    {
        @BeforeClass
        public static void setup()
        {
            Provisioner provisioner = new LocalProvisioner();
            ConfigurationManager configurationManager = new TestConfigurationManager();

            WritablePropertyGroup properties = new WritablePropertyGroup();

            startNodeGroup("local-provisioner", properties, provisioner, configurationManager);
        }
    }

    private Path writeFile(Path path, String contents)
    {
        Exceptions.runUncheckedIO(() -> Files.write(path, contents.getBytes(StandardCharsets.UTF_8)));
        return path;
    }

    @Test
    public void single_file_transfers_work()
    {
        final Path outgoing = createLocalDir("outgoing");
        final Path incoming = createLocalDir("incoming");

        for (Node node : testGroup.getNodes())
        {
            final Path filename = Paths.get(String.format("single_file_%d.txt", node.getNodeGroupOrdinal()));

            final Path outgoingPath = createLocalFile(outgoing.resolve(filename), TEMPFILE_CONTENTS);

            assertThat(node.put(outgoingPath, remotePath(filename, node.getNodeGroupOrdinal()), false).join()).isTrue();

            final Path incomingPath = incoming.resolve(filename);
            assertThat(node.get(remotePath(filename, node.getNodeGroupOrdinal()), incomingPath, false).join()).isTrue();

            assertThat(incomingPath).usingCharset(StandardCharsets.UTF_8).hasContent(TEMPFILE_CONTENTS);
        }
    }

    @Test
    public void single_directory_transfers_work() throws IOException
    {
        final Path outgoing = createLocalDir("outgoing");

        for (int i = 0; i < 3; i++)
        {
            createLocalFile(outgoing.resolve(String.format("file_%d.txt", i)), TEMPDIR_CONTENTS);
        }

        for (Node node : testGroup.getNodes())
        {
            final String remoteDir = remotePath(currentTestShortName(), node.getNodeGroupOrdinal());

            assertThat(node.put(outgoing, remoteDir, false).join()).isTrue();

            final Path incoming = createLocalDir("incoming")
                .resolve(String.format("node%s", node.getNodeGroupOrdinal()));

            assertThat(node.get(remoteDir, incoming, false).join()).isTrue();

            assertThat(Files.list(incoming))
                .allSatisfy(path -> assertThat(path).usingCharset(StandardCharsets.UTF_8).hasContent(TEMPDIR_CONTENTS));
        }
    }

    private void checkDeepCopySubdirBehaviour(boolean outgoingIsDeep, boolean incomingIsDeep) throws IOException
    {
        final Path subdir = Paths.get("subdir");
        final Path outgoing = createLocalDir("outgoing");

        for (int i = 0; i < 3; i++)
        {
            createLocalFile(outgoing.resolve(subdir).resolve(String.format("file_%d.txt", i)), TEMPDEEPDIR_CONTENTS);
        }

        for (Node node : testGroup.getNodes())
        {
            final String remoteDir = remotePath(currentTestShortName(), node.getNodeGroupOrdinal());

            assertThat(node.put(outgoing, remoteDir, outgoingIsDeep).join()).isTrue();

            final Path incoming = createLocalDir("incoming")
                .resolve(String.format("node%s", node.getNodeGroupOrdinal()));

            assertThat(node.get(remoteDir, incoming, incomingIsDeep).join()).isTrue();

            if (incomingIsDeep && outgoingIsDeep)
            {
                assertThat(Files.list(incoming.resolve(subdir))).allSatisfy(
                    path -> assertThat(path).usingCharset(StandardCharsets.UTF_8).hasContent(TEMPDEEPDIR_CONTENTS));
            }
            else
            {
                assertThat(incoming.resolve(subdir)).doesNotExist();
            }
        }
    }

    @Test
    public void deep_copy_single_directory_transfers_work() throws IOException
    {
        checkDeepCopySubdirBehaviour(true, true);
    }

    @Test
    public void shallow_copy_outgoing_single_directory_transfers_do_not_copy_subdirs() throws IOException
    {
        checkDeepCopySubdirBehaviour(false, true);
    }

    @Test
    public void shallow_copy_incoming_single_directory_transfers_do_not_copy_subdirs() throws IOException
    {
        checkDeepCopySubdirBehaviour(true, false);
    }

    @Test
    public void input_stream_transfers_work() throws IOException
    {
        for (Node node : testGroup.getNodes())
        {
            final String remotePath = remotePath(currentTestShortName(), node.getNodeGroupOrdinal());

            try (InputStream inputStream = new ByteArrayInputStream(TEMPFILE_CONTENTS.getBytes(StandardCharsets.UTF_8)))
            {
                assertThat(node.put(inputStream, remotePath, 0644).join()).isTrue();
            }

            final Path incoming = createLocalDir("incoming")
                .resolve(String.format("node%s.txt", node.getNodeGroupOrdinal()));

            assertThat(node.get(remotePath, incoming, false).join()).isTrue();

            assertThat(incoming).usingCharset(StandardCharsets.UTF_8).hasContent(TEMPFILE_CONTENTS);
        }
    }

    @Test
    public void unreadable_files_do_not_prevent_other_files_from_transferring() throws IOException
    {
        for (Node node : testGroup.getNodes())
        {
            final Path outgoing = createLocalDir("outgoing");
            final Path subdir = Paths.get("subdir");

            for (int i = 0; i < 3; i++)
            {
                createLocalFile(outgoing.resolve(String.format("file_%d.txt", i)), TEMPDIR_CONTENTS);
            }

            for (int i = 0; i < 3; i++)
            {
                createLocalFile(outgoing.resolve(subdir).resolve(String.format("file_%d.txt", i)),
                    TEMPDEEPDIR_CONTENTS);
            }

            final String remoteDir = remotePath(currentTestShortName(), node.getNodeGroupOrdinal());

            assertThat(node.put(outgoing, remoteDir, true).join()).isTrue();

            makeUnreadable(node, Paths.get(remoteDir).resolve("file_1.txt"));

            final Path incoming = createLocalDir("incoming")
                .resolve(String.format("node%s", node.getNodeGroupOrdinal()));

            // The operation should fail, but it will get as many files as it can
            assertThat(node.get(remoteDir, incoming, true).join()).isFalse();

            assertThat(Files.list(incoming).map(Path::getFileName).map(Path::toString))
                .containsExactlyInAnyOrder("file_0.txt", "file_2.txt", subdir.getFileName().toString());

            assertThat(Files.list(incoming.resolve(subdir)).map(Path::getFileName).map(Path::toString))
                .containsExactlyInAnyOrder("file_0.txt", "file_1.txt", "file_2.txt");
        }
    }
}
