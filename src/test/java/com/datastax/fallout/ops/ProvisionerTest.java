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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.sshd.server.SshServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.common.provisioner.SshOnlyProvisioner;
import com.datastax.fallout.components.impl.DummySshServerFactory;
import com.datastax.fallout.components.impl.TestConfigurationManager;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.ops.NodeGroup.State.STARTED_SERVICES_RUNNING;
import static com.datastax.fallout.ops.NodeGroupHelpers.assertSuccessfulTransition;
import static com.datastax.fallout.ops.NodeGroupHelpers.destroying;
import static com.datastax.fallout.ops.NodeGroupHelpers.waitForTransition;

/**
 * Tests for Provisioners
 */
public class ProvisionerTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    private Provisioner provisioner = new SshOnlyProvisioner();
    private ConfigurationManager configurationManager = new TestConfigurationManager();
    private static SshServer sshd;

    @BeforeAll
    public static void setup() throws Exception
    {
        //Start a provisioner sshd
        sshd = DummySshServerFactory.createServer();
    }

    @AfterAll
    public static void teardown() throws Exception
    {
        sshd.stop();
    }

    @Test
    public void testProvisionerValidate()
    {
        WritablePropertyGroup properties = new WritablePropertyGroup();

        NodeGroup testGroup = NodeGroupBuilder
            .create()
            .withName("test group")
            .withProvisioner(provisioner)
            .withConfigurationManager(configurationManager)
            .withPropertyGroup(properties)
            .withNodeCount(10)
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .build();

        try (NodeGroupHelpers.Destroyer destroyer = destroying(testGroup))
        {

            //Should fail without host
            assertThat(waitForTransition(testGroup, STARTED_SERVICES_RUNNING)).wasNotSuccessful();

            properties.put(DummySshServerFactory.createProperties());

            assertThat(waitForTransition(testGroup, STARTED_SERVICES_RUNNING)).wasSuccessful();
        }
    }

    @Test
    public void testSSHOnlyProvisioner() throws Exception
    {
        WritablePropertyGroup properties = DummySshServerFactory.createProperties();

        SshOnlyProvisioner provisioner = new SshOnlyProvisioner();

        NodeGroup testGroup = NodeGroupBuilder
            .create()
            .withName("test group")
            .withProvisioner(provisioner)
            .withConfigurationManager(configurationManager)
            .withPropertyGroup(properties)
            .withNodeCount(10)
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .build();

        try (NodeGroupHelpers.Destroyer destroyer = destroying(testGroup))
        {
            assertSuccessfulTransition(testGroup, STARTED_SERVICES_RUNNING);

            List<FullyBufferedNodeResponse> execResponses = new ArrayList<>();
            List<NodeResponse> execBadResponses = new ArrayList<>();

            assertThat(testGroup.getState()).isSameAs(NodeGroup.State.STARTED_SERVICES_RUNNING);

            for (Node node : testGroup.getNodes())
            {
                execResponses.add(node.executeBuffered("exit 0"));
                execBadResponses.add(node.execute("exit -1"));
            }

            //Validate a good command
            for (FullyBufferedNodeResponse response : execResponses)
            {
                assertThat(response.waitForSuccess()).isTrue();
                assertThat(response.getExitCode()).isEqualTo(0);
                for (String line : response.getStdout().split("\\n"))
                {
                    assertThat(line).startsWith("STDOUT");
                }
                for (String line : response.getStderr().split("\\n"))
                {
                    assertThat(line).startsWith("STDERR");
                }
            }

            //Test a bad result
            for (NodeResponse resp : execBadResponses)
            {
                while (!resp.isCompleted())
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

                assertThat(resp.getExitCode()).isNotEqualTo(0);
            }
        }
    }
}
