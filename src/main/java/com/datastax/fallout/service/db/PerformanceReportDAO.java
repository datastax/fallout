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
package com.datastax.fallout.service.db;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.dropwizard.lifecycle.Managed;

import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.fallout.service.core.PerformanceReport;
import com.datastax.fallout.service.core.TestRunIdentifier;

public class PerformanceReportDAO implements Managed
{
    final CassandraDriverManager driverManager;
    Mapper<PerformanceReport> reportMapper;
    ReportAccessor allAccessor;

    @Accessor
    private interface ReportAccessor
    {
        @Query("SELECT * FROM performance_reports WHERE email = :ownerEmail")
        Result<PerformanceReport> getAll(String ownerEmail);

        @Query("SELECT * FROM performance_reports")
        Result<PerformanceReport> getAll();
    }

    public PerformanceReportDAO(CassandraDriverManager driverManager)
    {
        this.driverManager = driverManager;
    }

    public List<PerformanceReport> getAll(String email)
    {
        return allAccessor.getAll(email).all();
    }

    public List<PerformanceReport> getAll()
    {
        return allAccessor.getAll().all();
    }

    public PerformanceReport get(String email, UUID reportId)
    {
        return reportMapper.get(email, reportId);
    }

    public void add(PerformanceReport report)
    {
        reportMapper.save(report);
    }

    public void delete(String email, UUID reportId)
    {
        reportMapper.delete(email, reportId);
    }

    @Override
    public void start() throws Exception
    {
        reportMapper = driverManager.getMappingManager().mapper(PerformanceReport.class);
        allAccessor = driverManager.getMappingManager().createAccessor(ReportAccessor.class);
    }

    @Override
    public void stop() throws Exception
    {

    }

    public static List<PerformanceReport> getPerformanceReportsContainingTestRun(PerformanceReportDAO reportDAO,
        Predicate<TestRunIdentifier> predicate)
    {
        return reportDAO.getAll().stream()
            .filter(perfReport -> perfReport.getReportTestRuns() != null &&
                perfReport.getReportTestRuns().stream()
                    .filter(Objects::nonNull)
                    .anyMatch(predicate))
            .collect(Collectors.toList());
    }
}
