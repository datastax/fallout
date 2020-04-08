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
package com.datastax.fallout.harness.artifact_checkers;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
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
import com.google.common.collect.ImmutableList;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramLogWriter;
import psy.lob.saw.HistogramIterator;
import psy.lob.saw.HistogramSink;
import psy.lob.saw.OrderedHistogramLogReader;
import psy.lob.saw.UnionHistograms;

import com.datastax.fallout.harness.ArtifactChecker;
import com.datastax.fallout.ops.Ensemble;
import com.datastax.fallout.ops.NodeGroup;
import com.datastax.fallout.ops.PropertySpec;
import com.datastax.fallout.ops.PropertySpecBuilder;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.service.core.TestRun;
import com.datastax.fallout.util.ShellUtils;
import com.datastax.fallout.util.TestRunUtils;

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

    private static boolean isHdrHistogram(String pathName)
    {
        return pathName.endsWith(".hgrm") ||
            (pathName.endsWith(".hdr") && !pathName.endsWith(UNION_FILE_EXT));
    }

    private static boolean isHdrHistogram(Path path)
    {
        return isHdrHistogram(path.toString());
    }

    @Override
    public List<PropertySpec> getPropertySpecs()
    {
        return ImmutableList.<PropertySpec>builder()
            .add(clientGroupSpec, reportNameSpec)
            .build();
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
        String reportTitle)
    {
        Map<String, List<File>> versionsToHdrFiles = new HashMap<>(testsToCompare.size());
        Map<TestRun, String> testRunDisplayNames = TestRunUtils.buildTestRunDisplayNames(testsToCompare);

        for (TestRun run : testsToCompare)
        {
            List<File> hdrFiles;
            hdrFiles = run.getArtifacts().keySet().stream()
                .filter(HdrHistogramChecker::isHdrHistogram)
                .map(s -> Paths
                    .get(rootArtifactPath, run.getOwner(), run.getTestName(), run.getTestRunId().toString(), s)
                    .toFile())
                .filter(f -> f.exists() && f.isFile())
                .collect(Collectors.toList());

            if (hdrFiles.isEmpty())
            {
                throw new RuntimeException(
                    "No HdrHistogram artifacts found for testrun: " + run.getTestName() + " " + run
                        .getTestRunId());
            }
            String displayName = testRunDisplayNames.get(run);
            versionsToHdrFiles.put(displayName, hdrFiles);
        }

        makeReport(Paths.get(rootArtifactPath, reportName), reportTitle, versionsToHdrFiles);
    }

    @VisibleForTesting
    void makeReport(Path reportPathWithoutExt, String title, Map<String, List<File>> versionsToHdrFiles)
    {
        makeReport(reportPathWithoutExt, title, versionsToHdrFiles, (s, h) -> {
        });
    }

    @VisibleForTesting
    void makeReport(
        Path reportPathWithoutExt,
        String title,
        Map<String, List<File>> versionsToHdrFiles,
        BiConsumer<String, Histogram> aggregatedHistogramConsumer)
    {
        //Make parent dirs
        reportPathWithoutExt.getParent().toFile().mkdirs();

        // open HTML and JSON output streams
        try (PrintStream htmlOutput = new PrintStream(new FileOutputStream(reportPathWithoutExt.toString() + ".html"));
            PrintStream jsonOutput = new PrintStream(new FileOutputStream(reportPathWithoutExt.toString() + ".json")))
        {
            htmlOutput.println(new String(Utils.getResource(this, "graph-header.html").get()));
            // stats array open
            htmlOutput.println("stats = {\"title\": \"" + title + "\",\"stats\":[");
            jsonOutput.println("{\"title\": \"" + title + "\",\"stats\":[");

            boolean moreThanOneVersion = false;

            for (Map.Entry<String, List<File>> entry : versionsToHdrFiles.entrySet())
            {
                // version keys can contain JSON reserved characters that need to be escaped
                final String versionKey = ShellUtils.escapeJSON(entry.getKey());
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
                        logger.warn("Files list for " + filename + " is empty.");
                        continue;
                    }
                    logger.info("Unioning Histograms from '" + filename + "': " + files);

                    List<Closeable> hdrFileReaders = new ArrayList<>(files.size());

                    // open hdr input file streams (as iterators)
                    try
                    {
                        List<HistogramIterator> ins = getIterators(files, hdrFileReaders);
                        if (ins.isEmpty())
                        {
                            logger.warn("Hdr logs listed for " + filename + " are all empty.");
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

                            unionIterators(ins, simpleFilename, versionKey, unionLogWriter, tagOutputMap,
                                fileStartTimeMs);

                            moreThanOneGroup = outputForTags(
                                tagOutputMap,
                                simpleFilename,
                                aggregatedHistogramConsumer,
                                htmlOutput,
                                jsonOutput,
                                moreThanOneGroup);

                            List<Histogram> summaries = tagOutputMap.values().stream().map(output -> output.sum)
                                .collect(Collectors.toList());
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
            htmlOutput.print(new String(Utils.getResource(this, "graph-footer.html").get()));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        // auto close HTML/JSON output streams
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
        String simpleFilename,
        BiConsumer<String, Histogram> aggregatedHistogramConsumer,
        PrintStream htmlOutput,
        PrintStream jsonOutput,
        boolean moreThanOneGroup)
        throws IOException
    {
        for (Map.Entry<String, TagOutput> entry : tagOutputMap.entrySet())
        {
            String tag = entry.getKey();
            TagOutput tagOutput = entry.getValue();
            PrintStream writer = tagOutput.out;
            Histogram sum = tagOutput.sum;
            ByteArrayOutputStream outputBytes = tagOutput.outputBytes;

            // terminate array
            writer.println("]");
            writeSummary(writer, sum);
            writer.println("}");
            String runTitle = simpleFilename + (tag == null ? "" : ":" + tag);

            // Write the aggregated histogram to a textual file
            aggregatedHistogramConsumer.accept(runTitle.replace(':', '.'), sum);

            moreThanOneGroup = outputMoreThanOneSeparator(htmlOutput, jsonOutput, moreThanOneGroup);
            outputBytes.writeTo(htmlOutput);
            outputBytes.writeTo(jsonOutput);
        }
        return moreThanOneGroup;
    }

    private void unionIterators(
        List<HistogramIterator> ins,
        String simpleFilename,
        String versionKey,
        HistogramLogWriter unionLogWriter,
        Map<String, TagOutput> tagOutputMap,
        long fileStartTimeMs)
    {
        UnionHistograms unionHistograms = new UnionHistograms(false, null, ins, new HistogramSink()
        {
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
                    tag -> {
                        TagOutput to = new TagOutput(union.getNumberOfSignificantValueDigits());
                        String runTitle = simpleFilename + (tag == null ? "" : ":" + tag);
                        printTagOutputHeader(versionKey, runTitle, to.out);
                        return to;
                    });
                tagOutput.sum.add(union);
                double intervalLengthSec = ((double) (union.getEndTimeStamp() - union.getStartTimeStamp())) * 0.001d;
                // json/html histogram timestamps are relative to the earliest file start time
                double timestampSec = ((double) (union.getEndTimeStamp() - fileStartTimeMs) * 0.001d);

                if (tagOutput.first)
                {
                    tagOutput.first = false;
                }
                else
                {
                    tagOutput.out.print(",");
                }
                printPercentilesLine(tagOutput.out, union, intervalLengthSec, timestampSec);
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
                OrderedHistogramLogReader reader = new OrderedHistogramLogReader(file);
                closeables.add(reader);
                ins.add(new HistogramIterator(reader, false));
            }
            catch (FileNotFoundException e)
            {
                throw new UncheckedIOException(e);
            }
        }
        ins.removeIf(e -> !e.hasNext());
        Collections.sort(ins);
        return ins;
    }

    @Override
    public boolean validate(Ensemble ensemble, Path rootArtifactLocation)
    {
        List<File> hdrfiles = listNodeGroupArtifacts(ensemble, rootArtifactLocation)
            .filter(Files::exists)
            .filter(Files::isRegularFile)
            .filter(HdrHistogramChecker::isHdrHistogram)
            .map(Path::toFile)
            .collect(Collectors.toList());

        if (hdrfiles.isEmpty())
        {
            logger.error("No HDR files found.");
            return false;
        }

        NodeGroup clientGroup = ensemble.getClientGroup(clientGroupSpec, getProperties());
        String reportNameStr = reportNameSpec.value(this);
        Path clientGroupPath = Paths.get(rootArtifactLocation.toString(), clientGroup.getName());
        Path reportPathWithoutExt = clientGroupPath.resolve(reportNameStr);
        Path aggregatesDir = clientGroupPath.resolve("aggregated-histograms");

        try
        {
            aggregatesDir.toFile().mkdir();
            BiConsumer<String, Histogram> aggregatedHistogramWriter =
                (fileName, histogram) -> writeAggregatedHistogram(aggregatesDir, fileName + ".txt", histogram);
            makeReport(reportPathWithoutExt, reportNameStr,
                Collections.singletonMap("Test Run", hdrfiles), aggregatedHistogramWriter);
        }
        catch (Throwable e)
        {
            logger.error("Unexpected exception in HdrHistogramChecker.validate", e);
            return false;
        }

        return true;
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
            logger.warn("Could not write aggregated histogram to " + targetPath);
        }
    }

    private Stream<Path> listNodeGroupArtifacts(Ensemble ensemble, Path rootArtifactLocation)
    {
        try
        {
            NodeGroup clientGroup = ensemble.getClientGroup(clientGroupSpec, getProperties());
            return Files.walk(rootArtifactLocation.resolve(clientGroup.getName()), FileVisitOption.FOLLOW_LINKS);
        }
        catch (IOException e)
        {
            logger.warn("Error while looking for HdrHistogram files", e);
            return Stream.empty();
        }
    }

    private void printTagOutputHeader(String version, String title, PrintStream writer)
    {
        writer.println(
            "{\"revision\":\"" + version + "\",\"test\":\"" + title +
                "\",\"metrics\":[\"Ops/Sec\",\"Latency Avg\",\"Latency Median\",\"Latency 95th\",\"Latency 99th\",\"Latency 99.9th\",\"Ops/Interval (like Cockpit)\",\"time\"],\"intervals\":[");
    }

    private long getHistogramThroughput(Histogram sum)
    {
        double intervalSec = ((double) (sum.getEndTimeStamp() - sum.getStartTimeStamp())) * .001;
        long opsPerSec = (long) ((((double) sum.getTotalCount()) / intervalSec));
        return opsPerSec;
    }

    private void writeSummary(PrintStream writer, Histogram sum)
    {
        writer.println(",\"Total Operations\": " + sum.getTotalCount());
        writer.println(String.format(",\"Op Rate\": \"%d op/sec\"", getHistogramThroughput(sum)));
        writer.println(String.format(",\"Min Latency\": \"%.3f ms\"", convertUnit(sum.getMinValue())));
        writer.println(String.format(",\"Avg Latency\": \"%.3f ms\"", convertUnit(sum.getMean())));
        writer.println(String.format(",\"Median Latency\": \"%.3f ms\"", convertUnit(sum.getValueAtPercentile(50.D))));
        writer.println(String.format(",\"95th Latency\": \"%.3f ms\"", convertUnit(sum.getValueAtPercentile(95.0D))));
        writer.println(String.format(",\"99th Latency\": \"%.3f ms\"", convertUnit(sum.getValueAtPercentile(99.0D))));
        writer.println(String.format(",\"99.9th Latency\": \"%.3f ms\"", convertUnit(sum.getValueAtPercentile(99.9D))));
        writer.println(String.format(",\"Max Latency\": \"%.3f ms\"", convertUnit(sum.getMaxValue())));
    }

    private void printPercentilesLine(
        PrintStream writer,
        Histogram union,
        double intervalSec,
        double timestampSec)
    {
        writer.println(String.format(
            Locale.ENGLISH,
            "[%.1f,%.3f,%.3f,%.3f,%.3f,%.3f,%d,%.2f]",
            ((double) union.getTotalCount()) / (intervalSec == 0.0 ? 1.0 : intervalSec),
            convertUnit(union.getMean()),
            convertUnit(union.getValueAtPercentile(50.0D)),
            convertUnit(union.getValueAtPercentile(95.0D)),
            convertUnit(union.getValueAtPercentile(99.0D)),
            convertUnit(union.getValueAtPercentile(99.9D)),
            union.getTotalCount(),
            timestampSec));
    }

    private double convertUnit(long value)
    {
        return value * OUTPUT_VALUE_UNIT_RATIO;
    }

    private double convertUnit(double value)
    {
        return value * OUTPUT_VALUE_UNIT_RATIO;
    }

    private static class TagOutput
    {
        final ByteArrayOutputStream outputBytes = new ByteArrayOutputStream(4096);
        final PrintStream out = new PrintStream(outputBytes);
        final Histogram sum;
        boolean first = true;

        TagOutput(int significantDigits)
        {
            sum = new Histogram(significantDigits);
        }
    }
}
