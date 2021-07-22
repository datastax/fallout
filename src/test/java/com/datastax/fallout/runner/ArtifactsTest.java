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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.datastax.fallout.TestHelpers;
import com.datastax.fallout.util.Exceptions;

import static com.datastax.fallout.assertj.Assertions.assertThat;
import static com.datastax.fallout.assertj.Assertions.assertThatCode;

class ArtifactsTest
{

    static class DisappearingArtifactsTest extends TestHelpers.ArtifactTest
    {
        private Thread artifactChaos;
        private AtomicBoolean stopChaos;
        private Path chaosDir;
        Set<String> completeExpectedArtifacts;

        @BeforeEach
        void startChaos()
        {
            chaosDir = testRunArtifactPath().resolve("dir");
            createChaosDir();
            completeExpectedArtifacts = findTestRunArtifacts();
            deleteChaosDir();
            stopChaos = new AtomicBoolean(false);
            artifactChaos = new Thread(this::createChaos);
            artifactChaos.start();
        }

        private Set<String> findTestRunArtifacts()
        {
            return Exceptions.getUncheckedIO(() -> Artifacts.findTestRunArtifacts(testRunArtifactPath())).keySet();
        }

        @AfterEach
        void stopChaos() throws InterruptedException
        {
            stopChaos.set(true);
            artifactChaos.join();
        }

        private void createDirectoryAndFiles(Path path, int depth)
        {
            if (depth == 0)
            {
                return;
            }

            Exceptions.runUncheckedIO(() -> {
                Files.createDirectories(path);
                for (int i = 0; i != 100; ++i)
                {
                    Files.createFile(path.resolve("f" + i));
                }
            });

            for (int i = 0; i != 5; ++i)
            {
                createDirectoryAndFiles(path.resolve("d" + i), depth - 1);
            }
        }

        private void createChaosDir()
        {
            createDirectoryAndFiles(chaosDir, 3);
        }

        private void deleteChaosDir()
        {
            Exceptions
                .runUncheckedIO(() -> MoreFiles.deleteRecursively(chaosDir, RecursiveDeleteOption.ALLOW_INSECURE));
        }

        private void createChaos()
        {
            while (!stopChaos.get())
            {
                createChaosDir();
                deleteChaosDir();
            }
        }

        @Test
        void artifact_collection_handles_disappearing_files()
        {
            assertThatCode(() -> {
                int maxArtifacts = 0;

                // Try to provoke an exception for at least 100 attempts, then
                // continue for a maximum of 1000 attempts to get a non-zero result
                for (var i = 0; i <= 100 || (i <= 1000 && maxArtifacts == 0); ++i)
                {
                    final var testRunArtifacts = findTestRunArtifacts();
                    assertThat(testRunArtifacts).isSubsetOf(completeExpectedArtifacts);
                    maxArtifacts = Math.max(maxArtifacts, testRunArtifacts.size());
                }

                assertThat(maxArtifacts)
                    .withFailMessage("At least one call resulted in _some_ artifacts")
                    .isGreaterThan(0);
            }).doesNotThrowAnyException();
        }
    }
}
