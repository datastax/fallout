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
package com.datastax.fallout.components.flamegraph;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.Utils;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;

import static java.lang.String.format;

public class FlamegraphProvider extends Provider
{
    final String flamegraphEnv;
    final String artifactBase;
    final List<FlamegraphProfile> flamegraphProfiles;

    protected FlamegraphProvider(Node node, String flamegraphEnv, String artifactBase)
    {
        super(node);

        this.flamegraphEnv = flamegraphEnv;
        this.artifactBase = artifactBase;
        this.flamegraphProfiles = new ArrayList<>();

        node.execute(String.format("mkdir -p %s", artifactBase)).waitForSuccess();
    }

    NodeResponse collectProfileData(String processIdGetter, String event, int seconds, String outputFile,
        boolean showLibrarySource)
    {
        flamegraphProfiles.add(new FlamegraphProfile(seconds, outputFile, showLibrarySource));
        final String outputData = artifactBase + File.separator + outputFile + ".data";
        // trigger a perf recording
        String cmd =
            format("export PERF_RECORD_FREQ=\"99 -e %s -v\"; export PERF_RECORD_SECONDS=%d; ", event, seconds) +
                format("export PERF_JAVA_TMP=%s; export PERF_DATA_FILE=%s; ", artifactBase, outputData) +
                format(". %s; perf-java-record-stack %s", flamegraphEnv, processIdGetter);

        return node().execute(cmd);
    }

    @Override
    public String name()
    {
        return "flamegraph";
    }

    @Override
    public Map<String, String> toInfoMap()
    {
        return Map.of("flamegraphEnv", flamegraphEnv,
            "flamegraphArtifacts", artifactBase);
    }

    public boolean postProcessData()
    {
        //When no data just skip
        if (flamegraphProfiles.isEmpty())
            return true;

        List<NodeResponse> nodeResponses = new ArrayList<>();
        for (FlamegraphProfile flamegraphProfile : flamegraphProfiles)
        {
            final String outputData = artifactBase + File.separator + flamegraphProfile.outputFile + ".data";
            final String outputStks = artifactBase + File.separator + flamegraphProfile.outputFile + ".stacks";
            final String outputCstk = artifactBase + File.separator + flamegraphProfile.outputFile + ".collapsed";
            final String outputSvgNoTid = artifactBase + File.separator + flamegraphProfile.outputFile + ".svg";
            final String outputSvg = artifactBase + File.separator + flamegraphProfile.outputFile + "-tid.svg";
            final String collapseOpts =
                flamegraphProfile.showLibrarySource ? "--inline --context --kernel --tid" : "--kernel --tid";

            String cmd =
                format(". %s; ", flamegraphEnv) +
                    // convert the binary perf .data file to a text format stacks log, then remove the .data file (not usable after this point)
                    format("sudo perf script -i %s > %s; sudo rm %s; ", outputData, outputStks, outputData) +
                    // collapse the stacks from the text format to the collapsed stacks format
                    format("$FLAMEGRAPH_DIR/stackcollapse-perf.pl %s %s > %s; ", collapseOpts, outputStks, outputCstk) +
                    "export CORES=`(grep -c ^processor /proc/cpuinfo)`; " +
                    // generate a thread level flamegraph, filtering the perf profiling noise
                    format("cat %s | egrep -v '__perf_event_task_sched_in|native_write_msr_safe' | " +
                        "$FLAMEGRAPH_DIR/flamegraph.pl --color=java --width=1850 --title=\"cores=$CORES time=%d\" > %s; ",
                        outputCstk, flamegraphProfile.seconds, outputSvg) +
                    // generate a process level flamegraph, filtering out profiling noise
                    format("cut -f2- -d';' %s | egrep -v '__perf_event_task_sched_in|native_write_msr_safe' | " +
                        "$FLAMEGRAPH_DIR/flamegraph.pl --color=java --width=1850 --title=\"cores=$CORES time=%d\" > %s; ",
                        outputCstk, flamegraphProfile.seconds, outputSvgNoTid);
            nodeResponses.add(node().execute(cmd));
        }

        return Utils.waitForQuietSuccess(logger(), nodeResponses, Duration.minutes(30));
    }

    private static class FlamegraphProfile
    {
        final String outputFile;
        final boolean showLibrarySource;
        final int seconds;

        public FlamegraphProfile(int seconds, String outputFile, boolean showLibrarySource)
        {
            this.seconds = seconds;
            this.outputFile = outputFile;
            this.showLibrarySource = showLibrarySource;
        }
    }
}
