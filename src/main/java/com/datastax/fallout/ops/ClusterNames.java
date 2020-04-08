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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.service.core.User;

public class ClusterNames
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterNames.class);

    private ClusterNames()
    {
        // utility class
    }

    /** Make a valid cluster name for GCE, which doesn't allow underscores or uppercase */
    public static String generateGCEClusterName(NodeGroup nodeGroup, String testRunName, TestRun testRun, User user)
    {
        return generateClusterName(nodeGroup, testRunName, testRun, user)
            .replace("_", "-")
            .toLowerCase();
    }

    public static String generateClusterName(NodeGroup nodeGroup, String testRunName, TestRun testRun, User user)
    {
        List<String> nameComponents = new ArrayList<>();
        if (testRun == null)
        {
            // unit test
            nameComponents.add(testRunName);
        }
        else
        {
            nameComponents.add(testRun.getTestName());
            nameComponents.add(testRun.buildShortTestRunId());
            if (nodeGroup.isMarkedForReuse())
            {
                nameComponents.add("nokill");
            }
        }
        nameComponents.add(nodeGroup.getId());
        return String.join("_", nameComponents);
    }
}
