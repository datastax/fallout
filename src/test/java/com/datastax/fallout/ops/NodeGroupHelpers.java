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

import com.datastax.fallout.runner.CheckResourcesResult;

import static com.datastax.fallout.runner.CheckResourcesResultAssert.assertThat;

public class NodeGroupHelpers
{
    public static CheckResourcesResult waitForTransition(NodeGroup nodeGroup, NodeGroup.State state)
    {
        return nodeGroup.transitionState(state).join();
    }

    public static void assertSuccessfulTransition(NodeGroup nodeGroup, NodeGroup.State state)
    {
        assertThat(waitForTransition(nodeGroup, state)).wasSuccessful();
    }

    public interface Destroyer extends AutoCloseable
    {
        @Override
        void close();
    }

    public static Destroyer destroying(NodeGroup nodeGroup)
    {
        return () -> assertSuccessfulTransition(nodeGroup, NodeGroup.State.DESTROYED);
    }
}
