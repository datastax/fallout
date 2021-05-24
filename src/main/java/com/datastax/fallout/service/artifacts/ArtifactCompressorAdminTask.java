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
package com.datastax.fallout.service.artifacts;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.dropwizard.servlets.tasks.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArtifactCompressorAdminTask extends Task
{
    private static final Logger logger = LoggerFactory.getLogger(ArtifactCompressorAdminTask.class);
    private final ArtifactCompressor artifactCompressor;

    public ArtifactCompressorAdminTask(ArtifactCompressor artifactCompressor)
    {
        super("artifact-compressor");
        this.artifactCompressor = artifactCompressor;
    }

    @Override
    public void execute(Map<String, List<String>> parameters, PrintWriter output) throws Exception
    {
        logger.info("Artifact compressor admin task invoked");
        CompletableFuture.runAsync(this::checkForUncompressedArtifacts);
    }

    private void checkForUncompressedArtifacts()
    {
        artifactCompressor.checkAllFinishedTestRunsForUncompressedArtifacts();
    }
}
