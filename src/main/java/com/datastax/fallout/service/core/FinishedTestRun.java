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
package com.datastax.fallout.service.core;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.fallout.ops.ResourceRequirement;

@Table(name = "finished_test_runs")
public class FinishedTestRun implements ReadOnlyTestRun
{
    @Column
    private String owner;

    @Column
    private String testName;

    @Column
    private UUID testRunId;

    @Column
    private Date createdAt;

    @Column
    private Date startedAt;

    @PartitionKey
    private Date finishedAtDate;

    @ClusteringColumn
    private Date finishedAt;

    @Column
    private TestRun.State state;

    @Column
    private TestRun.State failedDuring;

    @Column
    private Set<ResourceRequirement> resourceRequirements;

    public static FinishedTestRun fromTestRun(ReadOnlyTestRun testRun)
    {
        FinishedTestRun finishedTestRun = new FinishedTestRun();
        finishedTestRun.owner = testRun.getOwner();
        finishedTestRun.testName = testRun.getTestName();
        finishedTestRun.testRunId = testRun.getTestRunId();
        finishedTestRun.createdAt = testRun.getCreatedAt();
        finishedTestRun.startedAt = testRun.getStartedAt();
        finishedTestRun.setFinishedAt(testRun.getFinishedAt());
        finishedTestRun.state = testRun.getState();
        finishedTestRun.failedDuring = testRun.getFailedDuring();
        finishedTestRun.resourceRequirements = testRun.getResourceRequirements();
        return finishedTestRun;
    }

    private void setFinishedAt(Date finishedAt)
    {
        this.finishedAt = finishedAt;
        this.finishedAtDate = Date.from(finishedAt.toInstant().truncatedTo(ChronoUnit.DAYS));
    }

    private static final UUID END_STOP_UUID = UUID.fromString("7FA1891C-66D7-4286-A858-34B27C73772A");

    public static FinishedTestRun endStop(Date date)
    {
        FinishedTestRun finishedTestRun = new FinishedTestRun();
        finishedTestRun.setFinishedAt(date);
        finishedTestRun.testRunId = END_STOP_UUID;
        return finishedTestRun;
    }

    @Override
    public String getOwner()
    {
        return owner;
    }

    @Override
    public String getTestName()
    {
        return testName;
    }

    @Override
    public UUID getTestRunId()
    {
        return testRunId;
    }

    @Override
    public Date getCreatedAt()
    {
        return createdAt;
    }

    @Override
    public Date getStartedAt()
    {
        return startedAt;
    }

    @Override
    public Date getFinishedAt()
    {
        return finishedAt;
    }

    public Date getFinishedAtDate()
    {
        return finishedAtDate;
    }

    @Override
    public String getDefinition()
    {
        return null;
    }

    @Override
    public String getResults()
    {
        return null;
    }

    @Override
    public TestRun.State getState()
    {
        return state;
    }

    @Override
    public TestRun.State getFailedDuring()
    {
        return failedDuring;
    }

    @Override
    public Optional<Long> getArtifactsSizeBytes()
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Object> getTemplateParamsMap()
    {
        return Collections.emptyMap();
    }

    @Override
    public Set<ResourceRequirement> getResourceRequirements()
    {
        return resourceRequirements;
    }

    public boolean isEndStop()
    {
        return testRunId.equals(END_STOP_UUID);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        FinishedTestRun that = (FinishedTestRun) o;
        return Objects.equals(owner, that.owner) &&
            Objects.equals(testName, that.testName) &&
            Objects.equals(testRunId, that.testRunId) &&
            Objects.equals(createdAt, that.createdAt) &&
            Objects.equals(startedAt, that.startedAt) &&
            Objects.equals(finishedAtDate, that.finishedAtDate) &&
            Objects.equals(finishedAt, that.finishedAt) &&
            state == that.state &&
            failedDuring == that.failedDuring;
    }

    @Override
    public int hashCode()
    {
        return Objects
            .hash(owner, testName, testRunId, createdAt, startedAt, finishedAtDate, finishedAt, state, failedDuring);
    }

    @Override
    public String toString()
    {
        return "FinishedTestRun{" +
            "owner='" + owner + '\'' +
            ", testName='" + testName + '\'' +
            ", testRunId=" + testRunId +
            ", createdAt=" + createdAt +
            ", startedAt=" + startedAt +
            ", finishedAtDate=" + finishedAtDate +
            ", finishedAt=" + finishedAt +
            ", state=" + state +
            ", failedDuring=" + failedDuring +
            '}';
    }
}
