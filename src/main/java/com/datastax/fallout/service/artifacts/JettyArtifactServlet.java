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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.http.CompressedContentFormat;
import org.eclipse.jetty.http.HttpContent;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http.ResourceHttpContent;
import org.eclipse.jetty.server.ResourceService;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.util.resource.Resource;

import com.datastax.fallout.util.Exceptions;

import static org.eclipse.jetty.http.CompressedContentFormat.GZIP;

public class JettyArtifactServlet extends ArtifactServlet
{
    private final Resource resourceBase;
    private final ResourceService resourceService;
    private MimeTypes mimeTypes;

    public JettyArtifactServlet(Path resourceBasePath)
    {
        super(resourceBasePath);
        resourceBase = Exceptions.getUnchecked(() -> Resource.newResource(resourceBasePath.toString()));

        resourceService = new ResourceService();
        resourceService.setPathInfoOnly(true);
        resourceService.setDirAllowed(false);
        resourceService.setPrecompressedFormats(new CompressedContentFormat[] {GZIP});
        resourceService.setContentFactory(new GzipFallbackContentFactory());
    }

    @Override
    public void init() throws ServletException
    {
        super.init();
        mimeTypes = ContextHandler.getCurrentContext().getContextHandler().getMimeTypes();
    }

    @Override
    void getArtifact(HttpServletRequest req, HttpServletResponse resp,
        Path artifactPath, Path compressedArtifactPath,
        boolean rangeRequested) throws IOException, ServletException
    {
        resourceService.doGet(req, resp);
    }

    /**
     * This content factory will try to serve the gzip variant of a resource in case the original
     * resource does not exist.  This differs from the normal gzip handling in Jetty, which will
     * serve the gzip variant only if a) the original file exists, and b) there is a gzipped version.
     */
    private class GzipFallbackContentFactory implements HttpContent.ContentFactory
    {
        @Override
        public HttpContent getContent(String pathInContext, int maxBufferSize) throws IOException
        {
            Resource resource = resourceBase.getResource(pathInContext);

            if (resource.exists())
            {
                return new ResourceHttpContent(resource, mimeTypes.getMimeByExtension(pathInContext), maxBufferSize);
            }
            else
            {
                // partially copied from org.eclipse.jetty.server.ResourceContentFactory
                String compressedPathInContext = pathInContext + GZIP.getExtension();
                Resource compressedResource = resourceBase.getResource(compressedPathInContext);

                if (compressedResource.exists())
                {
                    String originalMimeType = mimeTypes.getMimeByExtension(pathInContext);
                    String compressedMimeType = mimeTypes.getMimeByExtension(compressedPathInContext);
                    Map<CompressedContentFormat, HttpContent> compressedContents = new HashMap<>();
                    compressedContents.put(GZIP,
                        new ResourceHttpContent(compressedResource, compressedMimeType, maxBufferSize));
                    return new ResourceHttpContent(
                        compressedResource, originalMimeType, maxBufferSize, compressedContents);
                }
            }

            return null;
        }
    }
}
