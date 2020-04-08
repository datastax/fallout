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
package com.datastax.fallout.service;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.stream.Collectors;

import io.dropwizard.jersey.errors.ErrorMessage;
import io.dropwizard.jersey.errors.LoggingExceptionMapper;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.LoggerFactory;

/** show stacktrace in server errors, so users can report internal errors without having to grep the server log */
class FalloutExceptionMapper<T extends Throwable> extends LoggingExceptionMapper<T>
{
    public FalloutExceptionMapper()
    {
        super(LoggerFactory.getLogger(FalloutExceptionMapper.class));
    }

    @Override
    public Response toResponse(T exception)
    {
        // If it's a WebApplicationException, use LoggingExceptionMapper's logic
        if (exception instanceof WebApplicationException)
        {
            return super.toResponse(exception);
        }

        // Otherwise, override it
        final long id = logException(exception);

        final String reducedStackTrace = Arrays.stream(ExceptionUtils.getStackFrames(exception))
            .filter(frame -> {
                final String trimmedFrame = frame.trim();
                return !trimmedFrame.startsWith("at") ||
                    trimmedFrame.contains("com.datastax.fallout");
            })
            .collect(Collectors.joining("\n"));

        return Response.status(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode())
            .type(MediaType.APPLICATION_JSON_TYPE)
            .entity(new ErrorMessage(
                Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
                String.format("%s (logged to fallout server log with ID %016x)",
                    exception.getLocalizedMessage() != null ? exception.getLocalizedMessage() : "Server error",
                    id),
                reducedStackTrace))
            .build();
    }
}
