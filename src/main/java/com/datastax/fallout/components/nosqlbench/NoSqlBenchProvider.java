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
package com.datastax.fallout.components.nosqlbench;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.datastax.fallout.ops.Node;
import com.datastax.fallout.ops.Provider;
import com.datastax.fallout.ops.commands.NodeResponse;
import com.datastax.fallout.util.Duration;

import com.google.common.base.Preconditions;

public abstract class NoSqlBenchProvider extends Provider
{
    public NoSqlBenchProvider(Node node)
    {
        super(node);
    }

    public abstract NodeResponse nosqlbench(String moduleName, String prepareScript, List<String> args,
        Duration histogramFrequency);

    protected abstract Path getBaseArtifactDir();

    protected String buildNosqlbenchArgs(String moduleName, List<String> args,
        Duration histogramFrequency)
    {
        String logFile = String.format("%s.log", moduleName);
        String hdrHistogram = String.format("%s.hdr", moduleName);
        String csvHistoStats = String.format("%s.csv", moduleName);
        String logsDir = String.format("nb-module-logs-%s", moduleName);

        Path baseArtifactDir = getBaseArtifactDir();
        logFile = baseArtifactDir.resolve(logFile).toString();
        hdrHistogram = baseArtifactDir.resolve(hdrHistogram).toString();
        csvHistoStats = baseArtifactDir.resolve(csvHistoStats).toString();
        logsDir = baseArtifactDir.resolve(logsDir).toString();

        List<String> argsCopy = new ArrayList<>(args);
        argsCopy.add(String.format("--logs-dir %s", logsDir));
        if (histogramFrequency.value != 0)
        {
            argsCopy.add(String.format("--log-histograms %s::%s", hdrHistogram, histogramFrequency.toAbbrevString()));
            argsCopy.add(String.format("--log-histostats %s::%s", csvHistoStats, histogramFrequency.toAbbrevString()));
        }
        String nosqlBenchArgs = String.join(" ", argsCopy);
        return String.format("%s 2>&1 | tee %s", nosqlBenchArgs, logFile);
    }

    protected abstract String fetchVersionInfo();


    public static class Version
    {

        public static Optional<Version> maybeOf(String versionStr)
        {
            try
            {
                return Optional.of(new Version(versionStr));
            }
            catch (IllegalArgumentException e)
            {
                // NumberFormatException extends IllegalArgumentException
                return Optional.empty();
            }
        }

        public final String versionStr;

        public final int major;
        public final int minor;
        public final int patch;
        public final String tag;

        public Version(String vString)
        {
            Preconditions.checkArgument(vString != null);
            vString = vString.replaceAll("[\\n\\t\\r\\s]+", "");

            versionStr = vString;

            int dashOffset = vString.indexOf("-");
            if (dashOffset != -1)
            {
                tag = vString.substring(dashOffset + 1);
                vString = vString.substring(0, dashOffset);
            }
            else
            {
                tag = "";
            }

            String[] parts = vString.split("\\.");

            major = Integer.valueOf(parts[0]);
            minor = parts.length > 1 ? Integer.valueOf(parts[1]) : -1;
            patch = parts.length > 2 ? Integer.valueOf(parts[2]) : -1;
        }

        public boolean isGTE(int major, int minor)
        {
            Preconditions.checkArgument(major > 0);
            Preconditions.checkArgument(minor >= 0);

            if (this.major < major)
                return false;

            if (this.major > major)
                return true;

            //Same major
            return this.minor >= minor;
        }

        public boolean isLTE(int major, int minor)
        {
            Preconditions.checkArgument(major > 0);
            Preconditions.checkArgument(minor >= 0);

            if (this.major > major)
                return false;

            if (this.major < major)
                return true;

            //Same major
            return this.minor <= minor;
        }

        public boolean isEquals(int major, int minor)
        {
            Preconditions.checkArgument(major > 0);
            Preconditions.checkArgument(minor >= 0);

            return this.major == major && this.minor == minor;
        }

        public String toString()
        {
            return versionStr;
        }
    }

}
