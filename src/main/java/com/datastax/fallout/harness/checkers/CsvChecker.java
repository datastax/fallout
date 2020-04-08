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
package com.datastax.fallout.harness.checkers;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.net.MediaType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.fallout.harness.Checker;
import com.datastax.fallout.harness.Operation;
import com.datastax.fallout.harness.specs.InMemoryCsvCheck;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.LocalScratchSpace;
import com.datastax.fallout.ops.PropertySpec;

/**
 * This class allows a CSV content stored in the history to be queried with an SQL request.
 *
 * To do so, that content is first written to a file named after the name given to its module.  Then, it is passed to
 * {@link InMemoryCsvCheck} for ingestion and validation.
 */
@AutoService(Checker.class)
public class CsvChecker extends Checker
{
    private static final String prefix = "fallout.checkers.csv.";

    private final InMemoryCsvCheck csvCheck = new InMemoryCsvCheck(prefix, () -> logger);

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
        return "Puts csv history output into in-memory SQL tables and query them";
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .addAll(csvCheck.getSpecs())
            .build();
    }

    @Override
    public boolean check(Ensemble ensemble, Collection<Operation> history)
    {
        List<Pair<String, File>> csvFiles = history.stream()
            .filter(this::hasSuccessType)
            .filter(this::containsCsvContent)
            .flatMap(op -> this.writeCsvToFile(op, ensemble.getLocalScratchSpace()))
            .collect(Collectors.toList());
        return csvCheck.checkQueryAgainst(csvFiles, getProperties());
    }

    private boolean containsCsvContent(Operation op)
    {
        return op.getMediaType().equals(MediaType.CSV_UTF_8) && //
            op.getValue() != null && //
            op.getValue() instanceof String;
    }

    private boolean hasSuccessType(Operation op)
    {
        return op.getType() == Operation.Type.ok || op.getType() == Operation.Type.info;
    }

    /**
     * Write the CSV content of {@code op} to a file if it is consistent and contains at least one record, distinct from
     * the header.
     *
     * @param op the {@link Operation} to extract the CSV content from
     *
     * @return a {@link Stream} of one {@link Pair} mapping the name of the process that created this operation to a
     * {@link File} holding its CSV content if the content was successfully parsed, an empty {@link Stream} otherwise
     */
    private Stream<Pair<String, File>> writeCsvToFile(Operation op, LocalScratchSpace withTempDir)
    {
        logger.debug("Writing CSV content of {} to file for analysis", op.getProcess());
        try
        {
            Character csvSeparator = csvCheck.separator(getProperties());
            CSVParser csvParser = new CSVParser( //
                new StringReader((String) op.getValue()),
                CSVFormat.newFormat(csvSeparator).withIgnoreEmptyLines(true).withHeader());
            List<CSVRecord> rows = csvParser.getRecords();
            // The header row has been parsed, we now expect at least 1 row and the same number of columns on all rows
            if (rows.size() == 0 || !rows.stream().allMatch(CSVRecord::isConsistent))
            {
                return Stream.empty();
            }
            File tempCsv = withTempDir.createFile("fallout", "csv").toFile();
            Files.write(((String) op.getValue()).getBytes(), tempCsv);
            return Stream.of(Pair.of(op.getProcess(), tempCsv));
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException("Cannot parse CSV content", e);
        }
    }
}
