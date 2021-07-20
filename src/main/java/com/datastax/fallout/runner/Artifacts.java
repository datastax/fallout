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
package com.datastax.fallout.runner;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.datastax.fallout.service.FalloutConfiguration;
import com.datastax.fallout.service.core.ReadOnlyTestRun;
import com.datastax.fallout.service.core.TestRun;

public class Artifacts
{
    public static Path buildTestArtifactPath(FalloutConfiguration configuration, ReadOnlyTestRun testRun)
    {
        return buildTestArtifactPath(configuration, testRun.getOwner(), testRun.getTestName());
    }

    public static Path buildTestArtifactPath(Path rootArtifactPath, ReadOnlyTestRun testRun)
    {
        return buildTestArtifactPath(rootArtifactPath, testRun.getOwner(), testRun.getTestName());
    }

    public static Path buildTestArtifactPath(Path rootArtifactPath, String userEmail, String testName)
    {
        return rootArtifactPath.resolve(Paths.get(userEmail, testName));
    }

    private static Path buildTestArtifactPath(FalloutConfiguration configuration, String userEmail, String testName)
    {
        return buildTestArtifactPath(Paths.get(configuration.getArtifactPath()), userEmail, testName);
    }

    public static Path buildTestRunArtifactPath(Path rootArtifactPath, ReadOnlyTestRun testRun)
    {
        return buildTestArtifactPath(rootArtifactPath, testRun).resolve(testRun.getTestRunId().toString());
    }

    public static Path buildTestRunArtifactPath(FalloutConfiguration configuration, TestRun testRun)
    {
        return buildTestRunArtifactPath(Paths.get(configuration.getArtifactPath()), testRun);
    }

    public static boolean hasStrippableGzSuffix(String pathname)
    {
        return pathname.endsWith(".gz") && !pathname.endsWith(".tar.gz");
    }

    public static String stripGzSuffix(String pathname)
    {
        return pathname.substring(0, pathname.length() - 3);
    }

    public static String maybeStripGzSuffix(String pathname)
    {
        return hasStrippableGzSuffix(pathname) ? stripGzSuffix(pathname) : pathname;
    }

    /** Get a map of <code>artifact -> size</code>, ignoring any {@link NoSuchFileException}, which
     *  allows us to function in the presence of concurrent file and directory removal/modification. */
    public static Map<String, Long> findTestRunArtifacts(Path testRunArtifactPath) throws IOException
    {
        if (!Files.isDirectory(testRunArtifactPath, LinkOption.NOFOLLOW_LINKS))
        {
            return Map.of();
        }

        final var artifacts = new HashMap<String, Long>();

        Files.walkFileTree(testRunArtifactPath, Set.of(FileVisitOption.FOLLOW_LINKS), 10,
            new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
                {
                    artifacts.put(testRunArtifactPath.relativize(path).toString(), attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                private FileVisitResult handleEx(IOException exc) throws IOException
                {
                    if (exc == null || exc instanceof NoSuchFileException)
                    {
                        return FileVisitResult.CONTINUE;
                    }
                    else
                    {
                        throw exc;
                    }
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException
                {
                    return handleEx(exc);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
                {
                    return handleEx(exc);
                }
            });

        return artifacts;
    }

    public static Map<String, Long> findTestRunArtifacts(FalloutConfiguration configuration, TestRun testRun)
        throws IOException
    {
        return findTestRunArtifacts(buildTestRunArtifactPath(configuration, testRun));
    }

    public static Map<String, Long> findTestRunArtifacts(Path rootArtifactPath, TestRun testRun) throws IOException
    {
        return findTestRunArtifacts(buildTestRunArtifactPath(rootArtifactPath, testRun));
    }
}
