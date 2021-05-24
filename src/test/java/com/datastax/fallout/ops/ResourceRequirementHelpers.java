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

import java.util.Optional;

public class ResourceRequirementHelpers
{
    public static ResourceRequirement req(String provider, String tenant, String instanceType, int nodeCount)
    {
        return new ResourceRequirement(
            new ResourceType(
                provider,
                tenant,
                instanceType,
                Optional.empty()),
            nodeCount);
    }

    public static ResourceRequirement req(String provider, String tenant, String instanceType, String uniqueName,
        int nodeCount)
    {
        return new ResourceRequirement(
            new ResourceType(
                provider,
                tenant,
                instanceType,
                Optional.of(uniqueName)),
            nodeCount);
    }
}
