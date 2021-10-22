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
package com.datastax.fallout.components.file_artifact_checkers;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.components.common.spec.InMemoryCsvCheck;
import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

import static java.util.stream.Collectors.toList;

/**
 * This class can be used to check a CSV file generated as an artifact with an SQL request.
 *
 * To do so, the file is passed to {@link InMemoryCsvCheck} for ingestion. That class provides the necessary
 * {@link PropertySpec} so that the user enters the SQL query to perform along with the expected result.
 */
@AutoService(ArtifactChecker.class)
public class CsvArtifactChecker extends ArtifactChecker
{
    private static final String prefix = "fallout.artifact_checkers.csvchecker.";
    private static final PropertySpec<String> fileSpec = PropertySpecBuilder.createStr(prefix)
        .name("file")
        .description("The name of the CSV file to analyse, should be unique in the artifact directory tree")
        .required()
        .build();

    private final InMemoryCsvCheck csvCheck = new InMemoryCsvCheck(prefix, () -> logger());

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "csvchecker";
    }

    @Override
    public String description()
    {
        return "Checks throughput CSV files against dynamic expressions";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .addAll(csvCheck.getSpecs())
            .add(fileSpec)
            .build();
    }

    @Override
    public boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        logger().info("Validating CSV files in {}", rootArtifactLocation);
        try
        {
            List<Pair<String, File>> matchingFiles;
            try (Stream<Path> pathStream = Files.walk(rootArtifactLocation))
            {
                matchingFiles = pathStream
                    .filter(this::fileNameMatchesFilter)
                    .map(this::toPair)
                    .collect(toList());
            }
            return csvCheck.checkQueryAgainst(matchingFiles, getProperties());
        }
        catch (IOException e)
        {
            logger().error("Cannot list files in " + rootArtifactLocation, e);
            return false;
        }
    }

    private boolean fileNameMatchesFilter(Path path)
    {
        return FileSystems.getDefault()
            .getPathMatcher("glob:" + fileSpec.value(this))
            .matches(path.getFileName());
    }

    private Pair<String, File> toPair(Path path)
    {
        String escapedTableName = path.getFileName().toString().replace('.', '_').replace('-', '_');
        return Pair.of(escapedTableName, path.toFile().getAbsoluteFile());
    }
}
