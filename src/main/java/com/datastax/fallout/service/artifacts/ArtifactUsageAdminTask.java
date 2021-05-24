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
package com.datastax.fallout.service.artifacts;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.dropwizard.servlets.tasks.Task;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import com.datastax.fallout.service.core.DeletedTestRun;
import com.datastax.fallout.service.db.TestRunDAO;
import com.datastax.fallout.util.Exceptions;

public class ArtifactUsageAdminTask extends Task
{
    private final TestRunDAO testRunDAO;

    public ArtifactUsageAdminTask(TestRunDAO testRunDAO)
    {
        super("artifact-usage", "text/csv");
        this.testRunDAO = testRunDAO;
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) throws Exception
    {
        writeArtifactUsage(output, parameters.containsKey("includeFiles"));
    }

    public void writeArtifactUsage(Appendable output, boolean includeFiles) throws IOException
    {
        final var testRuns = testRunDAO.getAllEvenIfDeleted();

        final var headers = new ArrayList<>(List.of("deleted", "owner", "test", "testrun", "finished"));

        if (includeFiles)
        {
            headers.addAll(List.of("dir", "file"));
        }

        headers.add("size");

        final var csvOutput = new CSVPrinter(output,
            CSVFormat.DEFAULT.withHeader(headers.toArray(new String[] {})));

        testRuns.forEach(testRun -> {
            if (includeFiles)
            {
                testRun.getArtifacts().forEach((artifact, size) -> {
                    final var path = Paths.get(artifact);
                    Exceptions.runUncheckedIO(() -> csvOutput
                        .printRecord(testRun instanceof DeletedTestRun,
                            testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
                            Optional.ofNullable(testRun.getFinishedAt()).map(Date::toInstant).orElse(null),
                            path.getParent(), path.getFileName(), size));
                });
            }
            else
            {
                Exceptions.runUncheckedIO(() -> csvOutput
                    .printRecord(testRun instanceof DeletedTestRun,
                        testRun.getOwner(), testRun.getTestName(), testRun.getTestRunId(),
                        Optional.ofNullable(testRun.getFinishedAt()).map(Date::toInstant).orElse(null),
                        testRun.getArtifactsSizeBytes().orElse(0L)));
            }
        });
    }
}
