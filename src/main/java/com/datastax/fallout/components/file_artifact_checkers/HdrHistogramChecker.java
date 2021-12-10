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

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import org.apache.commons.lang3.tuple.Pair;
import psy.lob.saw.HistogramIterator;
import psy.lob.saw.HistogramSink;
import psy.lob.saw.OrderedHistogramLogReader;
import psy.lob.saw.UnionHistograms;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.harness.EnsembleValidator;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.Exceptions;
import com.datastax.fallout.util.FileUtils;
import com.datastax.fallout.util.JsonUtils;
import com.datastax.fallout.util.ResourceUtils;
import com.datastax.fallout.util.TestRunUtils;
import com.datastax.fallout.util.cockpit.Aggregate;
import com.datastax.fallout.util.cockpit.CockpitAggregatesWriter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static psy.lob.saw.HdrHistogramUtil.createLogWriter;

/**
 * Aggregates HDR Histogram files together and generates cstar type report
 */
@AutoService(ArtifactChecker.class)
public class HdrHistogramChecker extends ArtifactChecker
{
    private static final double OUTPUT_VALUE_UNIT_RATIO = 0.000001d;
    private static final String prefix = "fallout.artifact_checkers.hdrhistogram.";
    private static final String UNION_FILE_EXT = ".union.hdr";
    private static final PropertySpec<String> clientGroupSpec = PropertySpecBuilder.clientGroup(prefix);
    private static final PropertySpec<String> reportNameSpec = PropertySpecBuilder.createStr(prefix, "[^\\/\\\\\\s]+")
        .name("report.prefix")
        .description("The prefix of the report output")
        .defaultOf("performance-report")
        .build();

    @Override
    public void validateEnsemble(EnsembleValidator validator)
    {
        validator.requireNodeGroup(clientGroupSpec);
    }

    private static boolean isHdrHistogram(String pathName)
    {
        String gzipSuffix = ".gz";
        if (pathName.endsWith(gzipSuffix))
        {
            pathName = pathName.substring(0, pathName.length() - gzipSuffix.length());
        }
        return pathName.endsWith(".hgrm") ||
            (pathName.endsWith(".hdr") && !pathName.endsWith(UNION_FILE_EXT));
    }

    private static boolean isHdrHistogram(Path path)
    {
        return isHdrHistogram(path.toString());
    }

    @Override
    public List<PropertySpec<?>> getPropertySpecs()
    {
        return List.of(clientGroupSpec, reportNameSpec);
    }

    @Override
    public String prefix()
    {
        return prefix;
    }

    @Override
    public String name()
    {
        return "hdrtool";
    }

    @Override
    public String description()
    {
        return "Merges hdr histograms and generate a cstar_perf report";
    }

    /**
     * Utility for PerformanceToolResource that can generate a multi test version of the report
     */
    public void compareTests(List<TestRun> testsToCompare, String rootArtifactPath, String reportName,
        String reportTitle, Path scratchDir)
    {
        Map<String, List<File>> versionsToHdrFiles = new HashMap<>(testsToCompare.size());
        Map<TestRun, String> testRunDisplayNames = TestRunUtils.buildTestRunDisplayNames(testsToCompare);

        List<File> allFilesToDeleteAfterwards = new ArrayList<>();
        for (TestRun run : testsToCompare)
        {
            List<Path> hdrPaths = run.getArtifacts().keySet().stream()
                .map(f -> Paths
                    .get(rootArtifactPath, run.getOwner(), run.getTestName(), run.getTestRunId().toString(), f)
                )
                .filter(HdrHistogramChecker::isHdrHistogram)
                .collect(toList());

            // HDR files from old tests could have been gzipped by the artifact compressor
            Pair<List<File>, List<File>> preparedHdrFiles = prepareTemporaryHdrFiles(hdrPaths, true);
            List<File> hdrFiles = preparedHdrFiles.getLeft();
            List<File> filesToDeleteAfterwards = preparedHdrFiles.getRight();
            allFilesToDeleteAfterwards.addAll(filesToDeleteAfterwards);
            if (hdrFiles.isEmpty())
            {
                cleanupTemporaryHdrFiles(allFilesToDeleteAfterwards);
                throw new RuntimeException(
                    "No HdrHistogram artifacts found for testrun: " + run.getTestName() + " " + run
                        .getTestRunId());
            }
            String displayName = testRunDisplayNames.get(run);
            versionsToHdrFiles.put(displayName, hdrFiles);
        }

        try
        {
            makeReport(Paths.get(rootArtifactPath, reportName), scratchDir, reportTitle, versionsToHdrFiles);
        }
        finally
        {
            cleanupTemporaryHdrFiles(allFilesToDeleteAfterwards);
        }
    }

    @VisibleForTesting
    boolean makeReport(Path reportPathWithoutExt, Path scratchDir, String title, Map<String,
        List<File>> versionsToHdrFiles)
    {
        return makeReport(reportPathWithoutExt, scratchDir, title, versionsToHdrFiles, (s, h) -> {});
    }

    @VisibleForTesting
    boolean makeReport(
        Path reportPathWithoutExt,
        Path scratchDir,
        String title,
        Map<String, List<File>> versionsToHdrFiles,
        BiConsumer<String, Histogram> aggregatedHistogramConsumer)
    {
        //Make parent dirs
        reportPathWithoutExt.getParent().toFile().mkdirs();

        List<Aggregate<Long>> historyAggregates = new ArrayList<>();

        boolean encounteredTooLargeHDR = false;

        // open HTML and JSON output streams
        try (PrintStream htmlOutput = new PrintStream(new FileOutputStream(reportPathWithoutExt.toString() + ".html"));
            PrintStream jsonOutput = new PrintStream(new FileOutputStream(reportPathWithoutExt.toString() + ".json")))
        {
            htmlOutput.println(ResourceUtils.getResourceAsString(getClass(), "graph-header.html"));
            // stats array open
            htmlOutput.println("stats = {\"title\": " + JsonUtils.toJson(title) + ",\"stats\":[");
            jsonOutput.println("{\"title\": " + JsonUtils.toJson(title) + ",\"stats\":[");

            boolean moreThanOneVersion = false;

            for (Map.Entry<String, List<File>> entry : versionsToHdrFiles.entrySet())
            {
                final String versionKey = entry.getKey();
                final List<File> hdrFiles = entry.getValue();

                moreThanOneVersion = outputMoreThanOneSeparator(htmlOutput, jsonOutput, moreThanOneVersion);

                // Find files across multiple clients with the same name and merge them
                // Sort by filename so that the dropdown follows ordered names the user used for the phases/modules
                TreeMap<String, List<File>> groupedFiles = hdrFiles
                    .stream()
                    .collect(Collectors.groupingBy(File::getName, TreeMap::new, toList()));

                boolean moreThanOneGroup = false;
                // This logic is mainly from HdrLogProcessing
                for (Map.Entry<String, List<File>> e : groupedFiles.entrySet())
                {
                    String filename = e.getKey();
                    List<File> files = e.getValue();
                    if (files == null || files.isEmpty())
                    {
                        logger().warn("Files list for " + filename + " is empty.");
                        continue;
                    }
                    logger().info("Unioning Histograms from '" + filename + "': " + files);

                    List<Closeable> hdrFileReaders = new ArrayList<>(files.size());

                    // open hdr input file streams (as iterators)
                    try
                    {
                        List<HistogramIterator> ins = getIterators(files, hdrFileReaders);
                        if (ins.isEmpty())
                        {
                            logger().warn("Hdr logs listed for " + filename + " are all empty.");
                        }
                        Map<String, TagOutput> tagOutputMap = new HashMap<>();
                        // iterators are sorted by the first histogram timestamp, but we want the min FILE start time
                        final double fileStartTimeSec =
                            ins.stream().mapToDouble(hi -> hi.getStartTimeSec()).min().orElse(0);
                        final long fileStartTimeMs = (long) (fileStartTimeSec * 1000);
                        String simpleFilename = filename.substring(0, filename.indexOf("."));
                        // open the union hdr log stream
                        String unionHdrLogFilename =
                            reportPathWithoutExt.getParent().resolve(simpleFilename + UNION_FILE_EXT).toString();
                        try (PrintStream hdrOut = new PrintStream(new FileOutputStream(unionHdrLogFilename)))
                        {
                            HistogramLogWriter unionLogWriter = createLogWriter(hdrOut, null, fileStartTimeSec);

                            unionIterators(scratchDir,
                                ins, simpleFilename, versionKey, unionLogWriter, tagOutputMap,
                                fileStartTimeMs);

                            moreThanOneGroup = outputForTags(
                                tagOutputMap,
                                aggregatedHistogramConsumer,
                                htmlOutput,
                                jsonOutput,
                                moreThanOneGroup);

                            List<Histogram> summaries = tagOutputMap.values().stream().map(output -> output
                                .getAggregatedHistogram())
                                .collect(Collectors.toList());
                            historyAggregates.addAll(getHistoryAggregates(simpleFilename, summaries));
                        }
                        // auto close union output stream
                    }
                    finally
                    {
                        // close hdr input streams
                        for (Closeable c : hdrFileReaders)
                        {
                            c.close();
                        }
                    }
                }
            }
            // stats array terminator
            htmlOutput.println("]};");
            jsonOutput.println("]}");

            // print the footer template
            htmlOutput.print(ResourceUtils.getResourceAsString(getClass(), "graph-footer.html"));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        // auto close HTML/JSON output streams

        // Write the history aggregates
        Path aggregatesDir = reportPathWithoutExt.getParent().resolve("aggregated-histograms");
        aggregatesDir.toFile().mkdirs();
        Path aggregatesFile = aggregatesDir.resolve("cockpit-aggregates.json");
        try
        {
            String aggregatesJson = CockpitAggregatesWriter.convertToJson(historyAggregates);
            Files.writeString(aggregatesFile, aggregatesJson);
        }
        catch (IOException e)
        {
            logger().warn("Unable to produce cockpit HDR aggregates", e);
        }

        return !encounteredTooLargeHDR;
    }

    private boolean outputMoreThanOneSeparator(PrintStream htmlOutput, PrintStream jsonOutput, boolean moreThanOne)
    {
        if (moreThanOne)
        {
            htmlOutput.println(",");
            jsonOutput.println(",");
        }
        else
        {
            moreThanOne = true;
        }
        return moreThanOne;
    }

    private boolean outputForTags(
        Map<String, TagOutput> tagOutputMap,
        BiConsumer<String, Histogram> aggregatedHistogramConsumer,
        PrintStream htmlOutput,
        PrintStream jsonOutput,
        boolean moreThanOneGroup)
        throws IOException
    {
        for (var tagOutput : tagOutputMap.values())
        {
            // Write the aggregated histogram to a textual file
            aggregatedHistogramConsumer.accept(
                tagOutput.getFileBaseName(),
                tagOutput.getAggregatedHistogram());

            moreThanOneGroup = outputMoreThanOneSeparator(htmlOutput, jsonOutput, moreThanOneGroup);
            tagOutput.writeJsonDataTo(htmlOutput, jsonOutput);
        }
        return moreThanOneGroup;
    }

    private void unionIterators(
        Path tmpDir,
        List<HistogramIterator> ins,
        String simpleFilename,
        String versionKey,
        HistogramLogWriter unionLogWriter,
        Map<String, TagOutput> tagOutputMap,
        long fileStartTimeMs)
    {
        UnionHistograms unionHistograms = new UnionHistograms(false, null, ins, new HistogramSink() {
            @Override
            public void startTime(double st)
            {
            }

            @Override
            public void accept(Histogram union)
            {
                // all tags go in chronological order
                unionLogWriter.outputIntervalHistogram(union);

                TagOutput tagOutput = tagOutputMap.computeIfAbsent(
                    union.getTag(),
                    tag -> new TagOutput(tmpDir, simpleFilename, versionKey, tag,
                        union.getNumberOfSignificantValueDigits(), fileStartTimeMs));

                tagOutput.addHistogram(union);
            }
        });
        unionHistograms.run();
    }

    private List<HistogramIterator> getIterators(List<File> hdrFiles, List<Closeable> closeables)
    {
        List<HistogramIterator> ins = new ArrayList<>(hdrFiles.size());
        for (File file : hdrFiles)
        {
            try
            {
                FileInputStream inputStream = new FileInputStream(file);
                closeables.add(inputStream);
                OrderedHistogramLogReader reader = new OrderedHistogramLogReader(inputStream);
                closeables.add(reader);
                ins.add(new HistogramIterator(reader, false));
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
        ins.removeIf(e -> !e.hasNext());
        Collections.sort(ins);
        return ins;
    }

    @Override
    public boolean checkArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        NodeGroup clientGroup = ensemble.getClientGroup(clientGroupSpec, getProperties());
        Path clientGroupPath = Paths.get(rootArtifactLocation.toString(), clientGroup.getName());

        String reportNameStr = reportNameSpec.value(this);
        Path reportPathWithoutExt = clientGroupPath.resolve(reportNameStr);
        Path aggregatesDir = clientGroupPath.resolve("aggregated-histograms");

        List<Path> hdrPaths = listNodeGroupArtifacts(clientGroupPath)
            .filter(HdrHistogramChecker::isHdrHistogram)
            .collect(toList());

        // no HDR files should be gzipped from a newly run test
        Pair<List<File>, List<File>> preparedHdrFiles = prepareTemporaryHdrFiles(hdrPaths, false);
        List<File> hdrFiles = preparedHdrFiles.getLeft();
        List<File> filesToDeleteAfterwards = preparedHdrFiles.getRight();
        if (hdrFiles.isEmpty())
        {
            logger().error("No HDR files found.");
            cleanupTemporaryHdrFiles(filesToDeleteAfterwards);
            return false;
        }

        try
        {
            aggregatesDir.toFile().mkdir();
            BiConsumer<String, Histogram> aggregatedHistogramWriter =
                (fileName, histogram) -> writeAggregatedHistogram(aggregatesDir, fileName + ".txt", histogram);
            boolean success = makeReport(
                reportPathWithoutExt,
                ensemble.makeScratchSpaceFor(this).getPath(),
                reportNameStr,
                Map.of("Test Run", hdrFiles),
                aggregatedHistogramWriter);
            if (!success)
            {
                logger().error("This test run encountered FAL-1451, marking FAILED");
            }
            return success;
        }
        catch (Throwable e)
        {
            logger().error("Unexpected exception in HdrHistogramChecker.validate", e);
            return false;
        }
        finally
        {
            cleanupTemporaryHdrFiles(filesToDeleteAfterwards);
        }
    }

    @VisibleForTesting
    public Pair<List<File>, List<File>> prepareTemporaryHdrFiles(List<Path> hdrPaths, boolean findGzipped)
    {
        // artifacts may have been compressed, so uncompress to generate a report and clean up afterwards
        final String gzipSuffix = ".gz";
        List<File> filesToUse = new ArrayList<>();
        List<File> filesToDeleteAfterwards = new ArrayList<>();
        for (Path hdrPath : hdrPaths)
        {
            String fileName = hdrPath.getFileName().toString();
            File file = hdrPath.toFile();
            boolean isGzipped = fileName.endsWith(gzipSuffix);
            if (file.exists() && file.isFile())
            {
                if (isGzipped)
                {
                    Path uncompressedPath = hdrPath
                        .resolveSibling(fileName.substring(0, fileName.length() - gzipSuffix.length()));
                    if (uncompressedPath.toFile().exists())
                    {
                        throw new RuntimeException(
                            "HDR Artifact exists both compressed and uncompressed: " + uncompressedPath
                                .toAbsolutePath());
                    }
                    Exceptions.runUnchecked(() -> FileUtils.uncompressGZIP(hdrPath, uncompressedPath));
                    File uncompressedFile = uncompressedPath.toFile();
                    filesToUse.add(uncompressedFile);
                    filesToDeleteAfterwards.add(uncompressedFile);
                    logger().info("Created temporary HDR artifact: " + uncompressedFile);
                }
                else
                {
                    filesToUse.add(file);
                }
                continue;
            }
            if (!isGzipped && findGzipped)
            {
                // artifact path stored in database may no longer match file on disk, so we need to detect gzipped files
                Path gzPath = hdrPath.resolveSibling(fileName + gzipSuffix);
                File gzFile = gzPath.toFile();
                if (gzFile.exists() && gzFile.isFile())
                {
                    Exceptions.runUnchecked(() -> FileUtils.uncompressGZIP(gzPath, hdrPath));
                    File uncompressedFile = hdrPath.toFile();
                    filesToUse.add(uncompressedFile);
                    filesToDeleteAfterwards.add(uncompressedFile);
                    logger().info("Created temporary HDR artifact: " + uncompressedFile);
                    continue;
                }
            }
            throw new RuntimeException("HDR Artifact path does not exist: " + hdrPath.toAbsolutePath());
        }
        if (hdrPaths.size() != filesToUse.size())
        {
            throw new RuntimeException("Unable to find all HDR Artifact paths: " + hdrPaths + "\nfound: " + filesToUse);
        }
        return Pair.of(filesToUse, filesToDeleteAfterwards);
    }

    @VisibleForTesting
    public void cleanupTemporaryHdrFiles(List<File> allFilesToDeleteAfterwards)
    {
        allFilesToDeleteAfterwards.forEach(f -> {
            logger().info("Deleting temporary HDR artifact: " + f);
            f.delete();
        });
    }

    @VisibleForTesting
    void writeAggregatedHistogram(Path directory, String filename, Histogram h)
    {
        Path targetPath = directory.resolve(filename);
        try (PrintStream printStream = new PrintStream(targetPath.toFile()))
        {
            h.outputPercentileDistribution(printStream, (double) MILLISECONDS.toNanos(1));
            long durationMillis = h.getEndTimeStamp() - h.getStartTimeStamp();
            double opRate = 0.0d;
            if (durationMillis > 0)
            {
                opRate = (h.getTotalCount() / (double) durationMillis) * 1000.0d;
            }
            printStream.printf("# Average throughput (ops/s) = %.0f%n", opRate);
            printStream.printf("# Start time = %d%n", MILLISECONDS.toSeconds(h.getStartTimeStamp()));
            printStream.printf("#   End time = %d%n", MILLISECONDS.toSeconds(h.getEndTimeStamp()));
            printStream.printf("# Period duration (ms) = %d%n", durationMillis);
        }
        catch (FileNotFoundException e)
        {
            logger().warn("Could not write aggregated histogram to " + targetPath);
        }
    }

    private Stream<Path> listNodeGroupArtifacts(Path nodeGroupArtifactsDir)
    {
        try
        {
            return Files.walk(nodeGroupArtifactsDir, FileVisitOption.FOLLOW_LINKS);
        }
        catch (IOException e)
        {
            logger().warn("Error while looking for HdrHistogram files", e);
            return Stream.empty();
        }
    }

    private static long getHistogramThroughput(Histogram sum)
    {
        double intervalSec = ((double) (sum.getEndTimeStamp() - sum.getStartTimeStamp())) * .001;
        long opsPerSec = (long) ((((double) sum.getTotalCount()) / intervalSec));
        return opsPerSec;
    }

    private List<Aggregate<Long>> getHistoryAggregates(String testPhase, List<Histogram> summaries)
    {
        List<Aggregate<Long>> historyAggregates = new ArrayList<>();
        for (Histogram sum : summaries)
        {
            List<Aggregate<Long>> aggregates = List.of(
                Aggregate.of("HDR", testPhase, "throughput", getHistogramThroughput(sum)),
                Aggregate.of("HDR", testPhase, "min", sum.getMinValue()),
                Aggregate.of("HDR", testPhase, "max", sum.getMaxValue()),
                Aggregate.of("HDR", testPhase, "avg", (long) sum.getMean()),
                Aggregate.of("HDR", testPhase, "p50", sum.getValueAtPercentile(50.D)),
                Aggregate.of("HDR", testPhase, "p90", sum.getValueAtPercentile(95.0D)),
                Aggregate.of("HDR", testPhase, "p99", sum.getValueAtPercentile(99.0D)),
                Aggregate.of("HDR", testPhase, "p999", sum.getValueAtPercentile(99.9D)),
                Aggregate.of("HDR", testPhase, "p9999", sum.getValueAtPercentile(99.99D))
            );
            historyAggregates.addAll(aggregates);
        }
        return historyAggregates;
    }

    private static double convertUnit(long value)
    {
        return value * OUTPUT_VALUE_UNIT_RATIO;
    }

    private static double convertUnit(double value)
    {
        return value * OUTPUT_VALUE_UNIT_RATIO;
    }

    /** Responsible for recording multiple unioned histograms for a single tag (via
     * {@link #addHistogram}), then making the data available as
     *
     * <ul>
     *     <li> a single aggregated histogram via {@link #getAggregatedHistogram};
     *     <li> and a JSON structure that lists key percentiles of each unioned histogram, which is written
     *          using {@link #writeJsonDataTo}.
     * </ul>
     */
    private static class TagOutput
    {
        private static PrintStream writer;
        private final PrintStream out;
        private final Histogram aggregatedHistogram;
        private final long fileStartTimeMs;
        private boolean firstLineWritten = true;

        private final String runTitle;
        private final Path tmpFile;

        TagOutput(Path tmpDir, String simpleFilename, String versionKey, String tag, int significantDigits,
            long fileStartTimeMs)
        {
            aggregatedHistogram = new Histogram(significantDigits);
            this.fileStartTimeMs = fileStartTimeMs;
            runTitle = simpleFilename + (tag == null ? "" : ":" + tag);
            tmpFile = tmpDir.resolve(getFileBaseName() + ".json");
            out = new PrintStream(
                Exceptions.getUncheckedIO(() -> Files.newOutputStream(tmpFile)));
            writeJsonHeader(runTitle, versionKey);
        }

        public void addHistogram(Histogram union)
        {
            aggregatedHistogram.add(union);

            double intervalLengthSec = ((double) (union.getEndTimeStamp() - union.getStartTimeStamp())) * 0.001d;

            // json/html histogram timestamps are relative to the earliest file start time
            double timestampSec = ((double) (union.getEndTimeStamp() - fileStartTimeMs) * 0.001d);

            writeJsonPercentilesLine(union, intervalLengthSec, timestampSec);
        }

        public Histogram getAggregatedHistogram()
        {
            return aggregatedHistogram;
        }

        /** Return a filename, without extensions, based on the simpleFilename and tag passed to the constructor */
        public String getFileBaseName()
        {
            return runTitle
                .replace(':', '.')
                .replace('/', '.');
        }

        public void writeJsonDataTo(PrintStream... outputs)
        {
            writeJsonTerminatorAndClose();
            Arrays.stream(outputs).forEach(output -> Exceptions.runUncheckedIO(() -> Files.copy(tmpFile, output)));
            Exceptions.runUncheckedIO(() -> Files.delete(tmpFile));
        }

        private void writeJsonHeader(String runTitle, String versionKey)
        {
            out.println(
                "{\"revision\":" + JsonUtils.toJson(versionKey) + ",\"test\":" + JsonUtils.toJson(runTitle) +
                    ",\"metrics\":[\"Ops/Sec\",\"Latency Avg\",\"Latency Median\",\"Latency 95th\",\"Latency 99th\",\"Latency 99.9th\",\"Ops/Interval (like Cockpit)\",\"time\"],\"intervals\":[");
        }

        private void writeJsonSeparator()
        {
            if (firstLineWritten)
            {
                firstLineWritten = false;
            }
            else
            {
                out.print(",");
            }
        }

        private void writeJsonPercentilesLine(
            Histogram union,
            double intervalSec,
            double timestampSec)
        {
            writeJsonSeparator();
            out.printf(
                Locale.ENGLISH,
                "[%.1f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%.2f]%n",
                ((double) union.getTotalCount()) / (intervalSec == 0.0 ? 1.0 : intervalSec),
                convertUnit(union.getMean()),
                convertUnit(union.getValueAtPercentile(50.0D)),
                convertUnit(union.getValueAtPercentile(95.0D)),
                convertUnit(union.getValueAtPercentile(99.0D)),
                convertUnit(union.getValueAtPercentile(99.9D)),
                union.getTotalCount(),
                timestampSec);
        }

        private void writeJsonSummary()
        {
            out.println(",\"Total Operations\": " + aggregatedHistogram.getTotalCount());
            out.printf(",\"Op Rate\": \"%d op/sec\"%n", getHistogramThroughput(aggregatedHistogram));
            out.printf(",\"Min Latency\": \"%.3f ms\"%n", convertUnit(aggregatedHistogram.getMinValue()));
            out.printf(",\"Avg Latency\": \"%.3f ms\"%n", convertUnit(aggregatedHistogram.getMean()));
            out.printf(",\"Median Latency\": \"%.3f ms\"%n",
                convertUnit(aggregatedHistogram.getValueAtPercentile(50.D)));
            out.printf(",\"95th Latency\": \"%.3f ms\"%n",
                convertUnit(aggregatedHistogram.getValueAtPercentile(95.0D)));
            out.printf(",\"99th Latency\": \"%.3f ms\"%n",
                convertUnit(aggregatedHistogram.getValueAtPercentile(99.0D)));
            out.printf(",\"99.9th Latency\": \"%.3f ms\"%n",
                convertUnit(aggregatedHistogram.getValueAtPercentile(99.9D)));
            out.printf(",\"Max Latency\": \"%.3f ms\"%n", convertUnit(aggregatedHistogram.getMaxValue()));
        }

        private void writeJsonTerminatorAndClose()
        {
            out.println("]");
            writeJsonSummary();
            out.println("}");
            out.close();
        }
    }
}
