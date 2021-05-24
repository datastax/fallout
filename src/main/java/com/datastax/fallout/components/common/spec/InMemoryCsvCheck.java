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
package com.datastax.fallout.components.common.spec;

import java.io.File;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.commons.lang3.tuple.Pair;
import org.h2.tools.Csv;
import org.slf4j.Logger;

import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

/**
 * This class provides several {@link com.datastax.fallout.ops.PropertySpec} for a CSV file to be loaded in an in-memory
 * SQL database and queried using SQL expressions.  It contains the following specs:
 *
 * <ul>
 * <li>The CSV separator that was used to generate the CSV file</li>
 * <li>The SQL query that should be performed against the file once loaded into the database</li>
 * <li>The expected value for the first column of the first line returned by the SQL query</li>
 * </ul>
 */
public class InMemoryCsvCheck
{
    private static final String H2_JDBC_URL = "jdbc:h2:mem:";
    private static final char DEFAULT_CSV_SEPARATOR = ',';
    private final PropertySpec<Character> csvSeparator;
    private final PropertySpec<List<String>> sqlQueries;
    private final PropertySpec<String> expectedValue;
    private final Supplier<Logger> logger;

    public InMemoryCsvCheck(String prefix, Supplier<Logger> logger)
    {
        csvSeparator = PropertySpecBuilder
            .<Character>create(prefix)
            .name("separator")
            .description("Separator to use to split the CSV columns")
            .defaultOf(DEFAULT_CSV_SEPARATOR)
            .parser(input -> {
                final var str = Objects.toString(input);
                Preconditions.checkArgument(str.length() == 1, "Must be a single character");
                return str.charAt(0);
            })
            .build();
        sqlQueries = PropertySpecBuilder
            .<List<String>>create(prefix)
            .required()
            .name("query")
            .description("SQL query to run against the CSV content. When used to check an artifact, the table " +
                "name must correspond to the file name, where dots and dashes are replaced by " +
                "underscores. Otherwise, it must match the name of an existing module that emitted CSV " +
                "content in its history. The column names must be derived from the first line of the " +
                "CSV output. All the values are loaded as strings, make sure to cast them before " +
                "aggregations are performed. Multiple queries may be separated by a pipe character")
            .parser(input -> Splitter.on('|').splitToList(input.toString()))
            .build();
        expectedValue = PropertySpecBuilder.createStr(prefix)
            .required()
            .name("expect")
            .description("The result to expect in the first column of the first row returned by the SQL query")
            .build();
        this.logger = logger;
    }

    public Collection<PropertySpec<?>> getSpecs()
    {
        return List.of(csvSeparator, sqlQueries, expectedValue);
    }

    public Character separator(PropertyGroup properties)
    {
        return csvSeparator.value(properties);
    }

    public boolean checkQueryAgainst(List<Pair<String, File>> csvFiles, PropertyGroup properties)
    {
        return importFiles(csvFiles, properties)
            .map(connection -> this.queriesMatchExpectation(connection, properties))
            .orElse(false);
    }

    private Optional<Connection> importFiles(List<Pair<String, File>> csvFiles, PropertyGroup properties)
    {
        logger.get().info("Importing files {}", csvFiles);
        // If there is no file to be analysed, do not return any connection
        if (csvFiles.isEmpty())
        {
            return Optional.empty();
        }
        try
        {
            Connection connection = DriverManager.getConnection(H2_JDBC_URL);
            for (Pair<String, File> table : csvFiles)
            {
                String importQuery = String.format(
                    "CREATE TABLE %s AS SELECT * FROM CSVREAD('%s', null, 'UTF-8', '%s');", //
                    table.getLeft(), //
                    table.getRight().getAbsolutePath(),
                    csvSeparator.value(properties));
                connection.createStatement().execute(importQuery);
            }
            return Optional.of(connection);
        }
        catch (SQLException e)
        {
            logger.get().error("Cannot import files into in-memory database", e);
            return Optional.empty();
        }
    }

    private boolean queriesMatchExpectation(Connection connection, PropertyGroup properties)
    {
        try
        {
            boolean resultMatchesExpectation = true;
            String expected = expectedValue.value(properties);
            for (String query : sqlQueries.value(properties))
            {
                ResultSet result = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY).executeQuery(query.trim());

                // Compare the expected value against the first column of the first row
                if (!result.next())
                {
                    return false;
                }
                resultMatchesExpectation = resultMatchesExpectation && expected.equalsIgnoreCase(result.getString(1));

                // Log query results for debugging purposes
                result.beforeFirst();
                StringWriter resultsAsCsv = new StringWriter();
                new Csv().write(resultsAsCsv, result);
                String[] lines = resultsAsCsv.toString().split("\n");
                logger.get().info("Query results ({} lines):", lines.length);
                for (String s : lines)
                {
                    logger.get().info(s);
                }

            }
            return resultMatchesExpectation;
        }
        catch (SQLException e)
        {
            logger.get().error("Cannot execute query", e);
            return false;
        }
        finally
        {
            try
            {
                connection.close();
            }
            catch (SQLException ignored)
            {
            }
        }
    }
}
