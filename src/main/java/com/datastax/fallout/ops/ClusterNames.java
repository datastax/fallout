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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRunIdentifier;
import com.datastax.fallout.service.core.User;

public class ClusterNames
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterNames.class);

    private ClusterNames()
    {
        // utility class
    }

    /** Make a valid cluster name for GCE, which doesn't allow underscores, uppercase, or names longer than 40 chars */
    public static String generateGCEClusterName(NodeGroup nodeGroup, TestRunIdentifier testRunIdentifier)
    {
        // limit of 40 required by the GKE API:
        // https://cloud.google.com/kubernetes-engine/docs/reference/rest/v1/projects.zones.clusters
        int MAX_NUM_CHARS_ALLOWED_FOR_GCE_CLUSTER_NAME = 40;
        return generateClusterName(nodeGroup, Optional.empty(), testRunIdentifier)
            .replace("_", "-")
            .toLowerCase()
            .substring(0, MAX_NUM_CHARS_ALLOWED_FOR_GCE_CLUSTER_NAME);
    }

    public static String generateClusterName(NodeGroup nodeGroup, Optional<User> user,
        TestRunIdentifier testRunIdentifier)
    {
        List<String> nameComponents = new ArrayList<>();

        if (user.isPresent())
        {
            Optional<String> userId = buildUserId(user.get());
            if (userId.isPresent())
            {
                nameComponents.add(userId.get());
            }
            else
            {
                logger.warn("buildClusterName - unable to determine userId for user: " + user);
            }
        }

        nameComponents.add(testRunIdentifier.getTestName());
        nameComponents.add(ReadOnlyTestRun.buildShortTestRunId(testRunIdentifier.getTestRunId()));

        if (nodeGroup.isMarkedForReuse())
        {
            nameComponents.add("nokill");
        }

        nameComponents.add(nodeGroup.getId());
        return String.join("_", nameComponents);
    }

    private static Optional<String> buildUserId(User user)
    {
        return Optional.ofNullable(user).map(User::getOrGenerateAutomatonSharedHandle);
    }
}
