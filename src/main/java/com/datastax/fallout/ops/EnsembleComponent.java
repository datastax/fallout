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

import com.google.common.base.Preconditions;

import com.datastax.fallout.harness.EnsembleValidator;

public abstract class EnsembleComponent implements PropertyBasedComponent, HasAvailableProviders
{
    private NodeGroup nodeGroup;

    public void setNodeGroup(NodeGroup nodeGroup)
    {
        Preconditions.checkState(this.nodeGroup == null);
        Preconditions.checkArgument(nodeGroup != null);
        this.nodeGroup = nodeGroup;
    }

    public NodeGroup getNodeGroup()
    {
        return nodeGroup;
    }

    public void validateEnsemble(EnsembleValidator validator)
    {
    }

    @Override
    public Set<Class<? extends Provider>> getAvailableProviders()
    {
        return getAvailableProviders(nodeGroup.getProperties());
    }

    abstract public Set<Class<? extends Provider>> getAvailableProviders(PropertyGroup propertyGroup);
}
