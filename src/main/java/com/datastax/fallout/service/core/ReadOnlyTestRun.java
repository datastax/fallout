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
package com.datastax.fallout.service.core;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.datastax.fallout.ops.ResourceRequirement;

public interface ReadOnlyTestRun extends HasPermissions
{
    String getTestName();

    UUID getTestRunId();

    static String buildShortTestRunId(UUID testRunId)
    {
        return testRunId.toString().split("-")[0];
    }

    default String buildShortTestRunId()
    {
        return buildShortTestRunId(getTestRunId());
    }

    Date getStartedAt();

    Date getCreatedAt();

    Date getFinishedAt();

    default Duration calculateDuration()
    {
        if (getCreatedAt() != null && this.getState().waiting())
            return Duration.between(getCreatedAt().toInstant(), Instant.now()); // time in queue
        else if (getStartedAt() != null && this.getState().active())
            return Duration.between(getStartedAt().toInstant(), Instant.now()); // time running
        else if (getStartedAt() != null && this.getState().finished() && this.getFinishedAt() != null)
            return Duration.between(getStartedAt().toInstant(), getFinishedAt().toInstant());
        else
            return null;
    }

    @SuppressWarnings("unused")
    default String calculateDurationString()
    {
        Duration d = calculateDuration();
        if (d == null)
            return null;

        String str = "";
        long absSeconds = Math.abs(d.getSeconds()); // assume > 0
        long h = absSeconds / 3600;
        long m = (absSeconds % 3600) / 60;
        long s = absSeconds % 60;
        if (h > 0)
            str = String.format("%s%dh ", str, h);
        if (m > 0)
            str = String.format("%s%dm ", str, m);
        if (s > 0 && h == 0) // only print seconds when there are no hours
            str = String.format("%s%ds", str, s);
        if (str.isEmpty())
            str = "0s";
        return str.trim();
    }

    String getDefinition();

    String getResults();

    TestRun.State getState();

    TestRun.State getFailedDuring();

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    Optional<Long> getArtifactsSizeBytes();

    Set<ResourceRequirement> getResourceRequirements();

    Map<String, String> getLinks();

    Map<String, Object> getTemplateParamsMap();

    boolean keepForever();

    default boolean belongsTo(Test test)
    {
        return test != null && getOwner().equals(test.getOwner()) && getTestName().equals(test.getName());
    }

    @JsonIgnore
    default TestRunIdentifier getTestRunIdentifier()
    {
        return new TestRunIdentifier(this.getOwner(), this.getTestName(), this.getTestRunId());
    }

    @JsonIgnore
    default String getShortName()
    {
        return String.format("%s %s %s", getOwner(), getTestName(), getTestRunId());
    }

}
