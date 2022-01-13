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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import psy.lob.saw.CsvConverter;
import psy.lob.saw.HistogramsSplitter;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.PropertyGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;

@AutoService(ArtifactChecker.class)
public class NbHdrSplitToCsvArtifactChecker extends ArtifactChecker
{
    private static final String prefix = "fallout.artifact_checkers.hdr_split_to_csv.";

    private final PropertySpec<List<String>> filesSpec = PropertySpecBuilder.createStrList(prefix)
        .name("files")
        .description("List of HDR histogram paths to process, relative to the fallout artifacts directory. All files " +
            "must have the '.hdr' extension.")
        .defaultOf(Collections.emptyList())
        .build();

    private final PropertySpec<List<String>> tagsSpec = PropertySpecBuilder.createStrList(prefix)
        .name("tags")
        .description("List of HDR histogram tags to keep and convert to CSV.")
        .defaultOf(Collections.emptyList())
        .build();

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "hdr_split_to_csv";
    }

    @Override
    public String description()
    {
        return "Splits and converts HDR histograms produced by NoSQLBench to CSV. This artifact checker adds the " +
            "resulting CSV files to the test run artifacts.";
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec<?>>builder()
            .add(filesSpec)
            .add(tagsSpec)
            .build();
    }

    @Override
    public void validateProperties(PropertyGroup properties) throws PropertySpec.ValidationException
    {
        List<String> files = filesSpec.value(properties);
        if (files.size() == 0)
        {
            throw new PropertySpec.ValidationException("Please specify at least one HDR histogram");
        }
        final List<String> tags = tagsSpec.value(properties);
        if (tags.size() == 0)
        {
            throw new PropertySpec.ValidationException("Please specify at least one tag");
        }
        List<String> invalidFiles = files
            .stream()
            .filter(fileName -> !fileName.endsWith(".hdr"))
            .toList();
        if (invalidFiles.size() != 0)
        {
            throw new PropertySpec.ValidationException("Found invalid files: " + invalidFiles);
        }
    }

    @Override
    public boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        logger().info("Splitting and converting HDR histograms to CSV");
        Collection<File> hdrFiles = findFilesToProcess(rootArtifactLocation, filesSpec.value(getProperties()));
        List<String> tags = tagsSpec.value(getProperties());
        for (File inputFile : hdrFiles)
        {
            boolean sourceIsZipped = inputFile.getName().endsWith(".gz");
            Collection<File> splitHdrFiles = splitHdrHistogram(inputFile, tags, sourceIsZipped);
            for (File splitHdrFile : splitHdrFiles)
            {
                convertToCsv(splitHdrFile, sourceIsZipped);
                // It is unnecessary to keep the split histograms. They already exist in the main HDR histogram, under
                // their respective tags.
                splitHdrFile.delete();
            }
        }
        logger().info("Done");
        return true;
    }

    @VisibleForTesting
    Collection<File> findFilesToProcess(Path rootArtifactLocation, List<String> fileNames)
    {
        List<File> filesToProcess = new ArrayList<>();
        for (String fileName : fileNames)
        {
            File uncompressedFile = rootArtifactLocation.resolve(fileName).toFile();
            File compressedFile = rootArtifactLocation.resolve(fileName + ".gz").toFile();
            if (uncompressedFile.exists())
            {
                filesToProcess.add(uncompressedFile);
            }
            else if (compressedFile.exists())
            {
                filesToProcess.add(compressedFile);
            }
            else
            {
                logger().warn("Could not find file '" + fileName + "' !");
            }
        }
        logger().info("Found " + filesToProcess.size() + " HDR files to split and convert");
        return filesToProcess;
    }

    @VisibleForTesting
    Collection<File> splitHdrHistogram(File inputFile, List<String> tagsToConvert, boolean isZipped)
    {
        // The input file may be gzipped if we are processing an old testrun. Since there is an `if` branch to decide
        // which input stream we read from, a try-with-resource cannot be used here.
        InputStream fileInputStream = null;
        InputStream gzipInputStream = null;
        try
        {
            fileInputStream = new FileInputStream(inputFile);
            InputStream inputStream;
            if (isZipped)
            {
                logger().info(inputFile + " is a gzipped file");
                gzipInputStream = new GZIPInputStream(fileInputStream);
                inputStream = gzipInputStream;
            }
            else
            {
                logger().info(inputFile + " is a regular file");
                inputStream = fileInputStream;
            }
            // Because of an implementation detail in HdrLogProcessing, the split HDR files will be named after
            // `inputFile.getName()`, which means they can end with `.hdr.gz` if the input file is compressed, despite
            // them not actually being gzipped.  It is acceptable though, given they are only temporary.
            Collection<File> result = new HistogramsSplitter(
                inputFile.getName(), inputStream, 0, Double.MAX_VALUE, false,
                o -> !tagsToConvert.contains(o), inputFile.toPath().getParent()
            ).split();
            logger().info("Split " + inputFile + " into " + result.size() + " temporary HDR histograms");
            return result;
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        finally
        {
            IOUtils.closeQuietly(gzipInputStream);
            IOUtils.closeQuietly(fileInputStream);
        }
    }

    @VisibleForTesting
    void convertToCsv(File hdrFile, boolean shouldZipCsv)
    {
        // If we read a compressed .hdr.gz file, then we are dealing with an old testrun which artifacts have been
        // compressed. We should therefore also compress the CSV files, as they will not be processed by the
        // ArtifactCompressor. As a result of this `if` branch, here as well, we cannot use a try-with-resource.
        InputStream inputStream = null;
        FileOutputStream fileOutputStream = null;
        GZIPOutputStream gzipOutputStream = null;
        PrintStream outputStream = null;
        try
        {
            inputStream = new FileInputStream(hdrFile);
            File outputFile;
            String fileName = hdrFile.getName();
            String fileNameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.'));
            if (shouldZipCsv)
            {
                outputFile = new File(hdrFile.getParent(), fileNameWithoutExt + ".csv.gz");
                fileOutputStream = new FileOutputStream(outputFile);
                gzipOutputStream = new GZIPOutputStream(fileOutputStream);
                outputStream = new PrintStream(gzipOutputStream);
            }
            else
            {
                outputFile = new File(hdrFile.getParent(), fileNameWithoutExt + ".csv");
                fileOutputStream = new FileOutputStream(outputFile);
                outputStream = new PrintStream(fileOutputStream);
            }
            new CsvConverter(inputStream, outputStream).convert();
            logger().info("Converted " + fileName + " into " + outputFile.getName());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        finally
        {
            IOUtils.closeQuietly(outputStream);
            IOUtils.closeQuietly(gzipOutputStream);
            IOUtils.closeQuietly(fileOutputStream);
            IOUtils.closeQuietly(inputStream);
        }
    }
}
