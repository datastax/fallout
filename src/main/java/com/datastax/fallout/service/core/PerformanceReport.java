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

import java.util.Date;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.fallout.util.DateUtils;

@Table(name = "performance_reports")
public class PerformanceReport
{
    @PartitionKey
    private String email;

    @ClusteringColumn
    private UUID reportGuid;

    @Column
    private Date reportDate;

    @Column
    private String reportName;

    @Column
    private Set<TestRunIdentifier> reportTestRuns;

    @Column
    private String reportArtifact;

    public String getEmail()
    {
        return email;
    }

    public void setEmail(String email)
    {
        this.email = email;
    }

    public UUID getReportGuid()
    {
        return reportGuid;
    }

    public void setReportGuid(UUID reportGuid)
    {
        this.reportGuid = reportGuid;
    }

    public Date getReportDate()
    {
        return reportDate;
    }

    public void setReportDate(Date reportDate)
    {
        this.reportDate = reportDate;
    }

    @JsonIgnore
    public String getReportDateUtc()
    {
        return DateUtils.formatUTCDate(getReportDate());
    }

    public String getReportName()
    {
        return reportName;
    }

    public void setReportName(String reportName)
    {
        this.reportName = reportName;
    }

    public Set<TestRunIdentifier> getReportTestRuns()
    {
        return reportTestRuns;
    }

    public void setReportTestRuns(Set<TestRunIdentifier> reportTestRuns)
    {
        this.reportTestRuns = reportTestRuns;
    }

    public String getReportArtifact()
    {
        return reportArtifact;
    }

    public void setReportArtifact(String reportArtifact)
    {
        this.reportArtifact = reportArtifact;
    }

}
