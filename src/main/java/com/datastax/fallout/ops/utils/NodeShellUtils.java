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
package com.datastax.fallout.ops.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.commands.FullyBufferedNodeResponse;

/**
 * A collection of shell related utility methods
 */
public class NodeShellUtils
{
    public static List<String> ls(Node node, String dir)
    {
        return ls(node, dir, null);
    }

    public static List<String> ls(Node node, String dir, String sudoCommand)
    {
        String sudoStr = sudoCommand != null ? sudoCommand : "";
        FullyBufferedNodeResponse contents = node.executeBuffered(String.format("%s ls %s/", sudoStr, dir));
        if (!contents.waitForSuccess())
        {
            node.logger().error("Couldn't list directory contents {}", dir);
            return Collections.emptyList();
        }
        return Arrays.asList(contents.getStdout().split("\\s+"));
    }
}
