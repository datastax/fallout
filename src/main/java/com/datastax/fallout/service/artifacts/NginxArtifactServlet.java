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

import org.eclipse.jetty.http.HttpHeader;

import static com.datastax.fallout.service.cli.GenerateNginxConf.NginxConfParams.NGINX_DIRECT_ARTIFACTS_LOCATION;
import static com.datastax.fallout.service.cli.GenerateNginxConf.NginxConfParams.NGINX_GZIP_ARTIFACTS_LOCATION;

public class NginxArtifactServlet extends ArtifactServlet
{
    public NginxArtifactServlet(Path resourceBase)
    {
        super(resourceBase);
    }

    @Override
    void getArtifact(HttpServletRequest req, HttpServletResponse resp,
        Path artifactPath, Path compressedArtifactPath, boolean rangeRequested) throws IOException
    {
        if (Files.exists(resourceBase().resolve(compressedArtifactPath)))
        {
            resp.setHeader("X-Accel-Redirect",
                NGINX_DIRECT_ARTIFACTS_LOCATION + "/" + compressedArtifactPath.toString());
            resp.setHeader(HttpHeader.CONTENT_ENCODING.asString(), "gzip");
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
                resp.setHeader("X-Accel-Redirect",
                    NGINX_DIRECT_ARTIFACTS_LOCATION + "/" + artifactPath.toString());
            }
            else
            {
                resp.setHeader("X-Accel-Redirect",
                    NGINX_GZIP_ARTIFACTS_LOCATION + "/" + artifactPath.toString());
            }
            resp.setContentType(getServletContext().getMimeType(req.getPathInfo()));
        }
        else
        {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not Found");
        }
    }
}
