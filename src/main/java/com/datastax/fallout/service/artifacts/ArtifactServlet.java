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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.eclipse.jetty.http.HttpHeader;

import static org.eclipse.jetty.http.CompressedContentFormat.GZIP;
import static org.eclipse.jetty.http.HttpStatus.FORBIDDEN_403;

public abstract class ArtifactServlet extends HttpServlet
{
    public static final String COMPRESSED_RANGE_REQUEST_ERROR_MESSAGE =
        "Fallout does not allow range requests on compressed artifacts";
    private final Path resourceBase;

    ArtifactServlet(Path resourceBase)
    {
        this.resourceBase = resourceBase;
    }

    Path resourceBase()
    {
        return resourceBase;
    }

    @Override
    protected final void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException
    {
        final Path artifactPath = Paths.get(req.getPathInfo().substring(1));
        final Path compressedArtifactPath = Paths.get(artifactPath.toString() + GZIP._extension);

        boolean rangeRequested = req.getHeader(HttpHeader.RANGE.asString()) != null;

        // Redirect requests for directories to the artifacts view
        if (Files.isDirectory(resourceBase.resolve(artifactPath)) && artifactPath.getNameCount() >= 3)
        {
            resp.sendRedirect("/tests/ui/" + artifactPath.subpath(0, 3).toString() + "/artifacts");
        }
        else if (rangeRequested && Files.exists(resourceBase.resolve(compressedArtifactPath)))
        {
            resp.sendError(FORBIDDEN_403, COMPRESSED_RANGE_REQUEST_ERROR_MESSAGE);
        }
        else
        {
            getArtifact(req, resp, artifactPath, compressedArtifactPath, rangeRequested);
        }
    }

    abstract void getArtifact(HttpServletRequest req, HttpServletResponse resp,
        Path artifactPath, Path compressedArtifactPath,
        boolean rangeRequested) throws IOException, ServletException;
}
