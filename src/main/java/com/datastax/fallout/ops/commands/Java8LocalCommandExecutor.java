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
package com.datastax.fallout.ops.commands;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;

import com.datastax.fallout.ops.Node;

/** CommandExecutor that sets <code>JAVA_HOME=$JAVA8_HOME</code>.
 *
 *  Why?  Many of the existing test tools will not work
 *  with Java 11, so this allows us to enforce the version as Java 8. */
public class Java8LocalCommandExecutor extends LocalCommandExecutor
{
    private static final String JAVA8_HOME =
        Preconditions.checkNotNull(System.getenv("JAVA8_HOME"), "$JAVA8_HOME must be set");

    @Override
    public NodeResponse executeLocally(Node owner,
        String command, Map<String, String> environment)
    {
        return super.executeLocally(owner, command, getJavaEnvironment(environment));
    }

    @Override
    public NodeResponse executeLocally(Logger logger, String command,
        Map<String, String> environment)
    {
        return super.executeLocally(logger, command, getJavaEnvironment(environment));
    }

    public Map<String, String> getJavaEnvironment(Map<String, String> environment)
    {
        return ImmutableMap.<String, String>builder()
            .putAll(environment)
            .put("JAVA_HOME", JAVA8_HOME)
            .build();
    }
}
