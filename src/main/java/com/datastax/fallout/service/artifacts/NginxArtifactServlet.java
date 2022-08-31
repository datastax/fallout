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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import org.eclipse.jetty.http.HttpHeader;

import static com.datastax.fallout.service.artifacts.ArtifactCompressor.isCompressibleArtifact;
import static com.datastax.fallout.service.cli.GenerateNginxConf.NginxConfParams.NGINX_ARTIFACT_ARCHIVE_LOCATION;
import static com.datastax.fallout.service.cli.GenerateNginxConf.NginxConfParams.NGINX_DIRECT_ARTIFACTS_LOCATION;
import static com.datastax.fallout.service.cli.GenerateNginxConf.NginxConfParams.NGINX_GZIP_ARTIFACTS_LOCATION;

public class NginxArtifactServlet extends ArtifactServlet
{
    private final Optional<ArtifactArchive> artifactArchive;

    public NginxArtifactServlet(Path resourceBase)
    {
        this(resourceBase, Optional.empty());
    }

    public NginxArtifactServlet(Path resourceBase, Optional<ArtifactArchive> artifactArchive)
    {
        super(resourceBase);
        this.artifactArchive = artifactArchive;
    }

    /**
     * Redirect header caught by the Nginx server.
     */
    private void setNginxRedirectHeader(HttpServletResponse resp, String s1)
    {
        resp.setHeader("X-Accel-Redirect", s1);
    }

    private void setGzipContentEncoding(HttpServletResponse resp)
    {
        resp.setHeader(HttpHeader.CONTENT_ENCODING.asString(), "gzip");
    }

    @Override
    void getArtifact(HttpServletRequest req, HttpServletResponse resp,
        Path artifactPath, Path compressedArtifactPath, boolean rangeRequested) throws IOException
    {
        if (Files.exists(resourceBase().resolve(compressedArtifactPath)))
        {
            setNginxRedirectHeader(resp, NGINX_DIRECT_ARTIFACTS_LOCATION + "/" + compressedArtifactPath.toString());
            setGzipContentEncoding(resp);
            resp.setContentType(getServletContext().getMimeType(req.getPathInfo()));
        }
        else if (Files.exists(resourceBase().resolve(artifactPath)))
        {
            if (rangeRequested)
            {
                // The NginX gzip module disables range requests, so we go through the direct location which
                // doesn't use the gzip module.  Note that the current implementation of live-artifact-view
                // will completely bypass fallout and access NGINX_DIRECT_ARTIFACTS_LOCATION, so this code should
                // not be executed; it's here for correctness.
                setNginxRedirectHeader(resp, NGINX_DIRECT_ARTIFACTS_LOCATION + "/" + artifactPath.toString());
            }
            else
            {
                setNginxRedirectHeader(resp, NGINX_GZIP_ARTIFACTS_LOCATION + "/" + artifactPath.toString());
            }
            resp.setContentType(getServletContext().getMimeType(req.getPathInfo()));
        }
        else if (artifactArchive.isPresent())
        {
            // We have ruled out the possibility the artifact exists on the local filesystem.
            // Allow  remote artifact archive to return 404 if artifact does not exist.
            String s3URL = artifactArchive.get().getArtifactUrl(artifactPath).toString()
                .replace("https://", "");
            String internalRedirect = NGINX_ARTIFACT_ARCHIVE_LOCATION + "/" + s3URL;
            setNginxRedirectHeader(resp, internalRedirect);

            if (isCompressibleArtifact(artifactPath))
            {
                // It is an error if a compressible artifact exists in the archive without first being gzipped.
                setGzipContentEncoding(resp);
            }
        }
        else
        {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not Found");
        }
    }
}
