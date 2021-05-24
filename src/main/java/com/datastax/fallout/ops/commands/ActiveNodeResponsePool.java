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
package com.datastax.fallout.ops.commands;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ActiveNodeResponsePool
{
    private final Set<NodeResponse> active = Collections.synchronizedSet(new HashSet<>());

    public NodeResponse addActive(NodeResponse nodeResponse)
    {
        active.add(nodeResponse);
        nodeResponse.addCompletionListener(active::remove);
        return nodeResponse;
    }

    public void killAll()
    {
        active.forEach(NodeResponse::kill);
    }
}
