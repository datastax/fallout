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
package com.datastax.fallout.components.common.provider;

import java.util.Map;
import java.util.Optional;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;

public class JavaProvider extends Provider
{
    private final Optional<String> javaVersion;

    public JavaProvider(Node node)
    {
        super(node);
        this.javaVersion = Optional.empty();
    }

    public JavaProvider(Node node, String javaVersion)
    {
        super(node);
        this.javaVersion = Optional.of(javaVersion);
    }

    @Override
    public String name()
    {
        return "java";
    }

    public Optional<String> version()
    {
        return this.javaVersion;
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        return Map.of("java_version", javaVersion.orElse("null"));
    }
}
