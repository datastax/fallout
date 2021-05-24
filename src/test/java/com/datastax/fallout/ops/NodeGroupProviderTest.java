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

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.components.fakes.FakeConfigurationManager;
import com.datastax.fallout.components.fakes.FakeProvider;
import com.datastax.fallout.components.fakes.FakeProvisioner;
import com.datastax.fallout.service.FalloutConfiguration;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class NodeGroupProviderTest extends TestHelpers.FalloutTest<FalloutConfiguration>
{
    private static class FakeConfigurationManagerWithProvider extends FakeConfigurationManager
    {
        @Override
        public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup nodeGroupProperties)
        {
            return Set.of(FakeProvider.class);
        }

        @Override
        public boolean registerProviders(Node node)
        {
            new FakeProvider(node);
            return true;
        }
    }

    @Test
    public void waitForProvider_blocks_until_provider_is_registered()
    {
        NodeGroup nodeGroup = NodeGroupBuilder.create()
            .withProvisioner(new FakeProvisioner())
            .withConfigurationManager(new FakeConfigurationManagerWithProvider())
            .withPropertyGroup(new WritablePropertyGroup())
            .withNodeCount(1)
            .withName("test")
            .withTestRunArtifactPath(testRunArtifactPath())
            .withTestRunScratchSpace(persistentTestScratchSpace())
            .build();

        final CompletableFuture<FakeProvider> waitForProviderFuture =
            CompletableFuture.supplyAsync(() -> nodeGroup.getNodes().get(0).waitForProvider(FakeProvider.class));

        assertThat(waitForProviderFuture).isNotCompleted();

        nodeGroup.transitionState(NodeGroup.State.STARTED_SERVICES_CONFIGURED);

        await().untilAsserted(() -> assertThat(waitForProviderFuture).isCompleted());
    }
}
