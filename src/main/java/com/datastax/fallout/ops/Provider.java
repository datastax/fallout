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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;

/**
 * A Provider 'provides' functionality/info related to a node and encapsulates
 * methods to it for the caller.
 *
 * An example of this would be a 'nodetool' provider
 *
 * This should really only be used for common tools
 *
 * @see Node#addProvider(Provider)
 *
 */
public abstract class Provider
{
    private final Node node;

    protected Provider(Node node)
    {
        this(node, true);
    }

    protected Provider(Node node, boolean autoRegister)
    {
        this.node = node;
        Set<Class<? extends Provider>> missingProviders =
            this.getRequiredProviders().stream().filter(c -> !node.hasProvider(c)).collect(Collectors.toSet());
        if (!missingProviders.isEmpty())
        {
            String msg = String.format("Failed to add %s to %s because required providers are missing: %s",
                this.getClass(), node.getId(), missingProviders);
            node.logger().error(msg);
            throw new IllegalStateException(msg);
        }
        if (autoRegister)
            node.addProvider(this);
    }

    public void register()
    {
        node.addProvider(this);
    }

    public void unregister()
    {
        node.removeProvider(this);
    }

    public abstract String name();

    public Logger logger()
    {
        return node.logger();
    }

    public Map<String, String> toInfoMap()
    {
        return Map.of();
    }

    public Node node()
    {
        return node;
    }

    public Set<Class<? extends Provider>> getRequiredProviders()
    {
        return Set.of();
    }
}
