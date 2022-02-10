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
package com.datastax.fallout.service.resources;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.sse.InboundSseEvent;
import javax.ws.rs.sse.OutboundSseEvent;
import javax.ws.rs.sse.Sse;
import javax.ws.rs.sse.SseEventSink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import com.datastax.fallout.util.ScopedLogger;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.MediaType.SERVER_SENT_EVENTS;

/** Abstracts starting an HTML5 Server Sent Events stream
 *
 *  This expands on JAXRS {@link SseEventSink} with close detection and a heartbeat. */
public class ServerSentEvents implements Managed
{
    private static final ScopedLogger logger = ScopedLogger.getLogger(ServerSentEvents.class);

    private final HashedWheelTimer scheduler;
    private final int heartBeatIntervalSeconds;
    private final Set<EventSink> activeSinks = new HashSet<>();

    private static Consumer<Boolean> streamOpenStatesListener = i -> {};

    public ServerSentEvents(HashedWheelTimer scheduler, int heartBeatIntervalSeconds)
    {
        this.scheduler = scheduler;
        this.heartBeatIntervalSeconds = heartBeatIntervalSeconds;
    }

    public static String eventName(Class<?> clazz)
    {
        var jsonSerializeAnnotation = clazz.getAnnotation(JsonSerialize.class);
        return jsonSerializeAnnotation != null ? jsonSerializeAnnotation.as().getSimpleName() : clazz.getSimpleName();
    }

    public static <T> OutboundSseEvent event(Sse sse, T object)
    {
        return sse.newEventBuilder()
            .name(eventName(object.getClass()))
            .data(object)
            .mediaType(APPLICATION_JSON_TYPE)
            .build();
    }

    public record EventConsumer<T> (Class<T> eventType, Consumer<T> consumer)
        implements
            Predicate<InboundSseEvent> {

        public static <T> EventConsumer<T> of(Class<T> eventType, Consumer<T> consumer)
        {
            return new EventConsumer<T>(eventType, consumer);
        }

        @Override
        public boolean test(InboundSseEvent event)
        {
            if (eventName(this.eventType()).equals(event.getName()))
            {
                this.consumer().accept(
                    logger.withResultDebug("readEvent({})", event).get(() -> event.readData(this.eventType())));
                return true;
            }
            logger.trace("readEvent({}): no matching consumers", event);
            return false;
        }
    }

    private static void readEvent(InboundSseEvent event, EventConsumer<?>... consumers)
    {
        Arrays.stream(consumers).anyMatch(consumer -> consumer.test(event));
    }

    /** Consumer that hands the event to each consumer in order until it is consumed */
    public static Consumer<InboundSseEvent> eventConsumers(EventConsumer<?>... consumers)
    {
        return event -> readEvent(event, consumers);
    }

    /** Consumer that hands the event to each consumer in order until it is consumed;
     *  exceptions are logged and squashed to prevent calling threads from being terminated */
    public static Consumer<InboundSseEvent> safeEventConsumers(EventConsumer<?>... consumers)
    {
        return event -> {
            try
            {
                readEvent(event, consumers);
            }
            catch (Throwable ex)
            {
                logger.error("Ignoring unexpected exception when consuming event " + event, ex);
            }
        };
    }

    @VisibleForTesting
    public static void setStreamOpenStatesListener(Consumer<Boolean> streamOpenStatesListener)
    {
        ServerSentEvents.streamOpenStatesListener = streamOpenStatesListener;
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
        synchronized (activeSinks)
        {
            logger.withScopedInfo("Closing active SSE connections").run(() -> {
                activeSinks.forEach(EventSink::close);
                activeSinks.clear();
            });
        }
    }

    public interface EventSource
    {
        void onOpen(EventSink eventSink);

        default void onClose()
        {
        }
    }

    private static class OpenStatesLoggingEventSource implements EventSource
    {
        private final EventSource delegate;

        private OpenStatesLoggingEventSource(EventSource delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void onOpen(EventSink eventSink)
        {
            streamOpenStatesListener.accept(true);
            delegate.onOpen(eventSink);
            logger.debug("{}.onOpen({})", this, eventSink);
        }

        @Override
        public void onClose()
        {
            delegate.onClose();
            streamOpenStatesListener.accept(false);
            logger.debug("{}.onClose()", this);
        }
    }

    public void startEventStream(HttpServletRequest request, HttpServletResponse response,
        SseEventSink eventSink, Sse sse, EventSource eventSource) throws IOException
    {
        respond(response);
        startEventStream(request, eventSink, sse, eventSource);
    }

    private void respond(HttpServletResponse response) throws IOException
    {
        // For some reason Jersey's SSE support doesn't set the content type
        response.setContentType(SERVER_SENT_EVENTS);
        // Tell Nginx not to buffer these responses and turn off client caching (see https://serverfault.com/questions/801628/for-server-sent-events-sse-what-nginx-proxy-configuration-is-appropriate):
        response.addHeader("X-Accel-Buffering", "no");
        response.addHeader("Cache-Control", "no-cache");
        response.flushBuffer();
    }

    private void startEventStream(HttpServletRequest request, SseEventSink sseEventSink, Sse sse,
        EventSource eventSource)
    {
        eventSource = new OpenStatesLoggingEventSource(eventSource);
        EventSink eventSink = new EventSink(sseEventSink, sse, eventSource, request);
        eventSource.onOpen(eventSink);
        // Don't schedule heartbeat until after onOpen, since onOpen is allowed to close the eventSink.  Send a
        // heartbeat straight away, so that SseEventSource.open() (which waits for an initial message) returns ASAP
        eventSink.sendAndRescheduleHeartbeat(null);
    }

    /** Jersey 2.29.1's implementation of {@link SseEventSink#send} is broken: the JAX 2.1.1 spec says it should
     * complete exceptionally on failure, but instead it returns a {@code CompletableFuture<IOException>} ( <a
     * href="https://github.com/eclipse-ee4j/jersey/blob/2.29.1/media/sse/src/main/java/org/glassfish/jersey/media/sse/internal/JerseyEventSink.java#L104">JerseyEventSink.java</a>).
     * Also, send is meant to be asynchronous, which is why it returns a CompletionStage, but the jersey implementation
     * blocks.  This class wraps SseEventSink.send to give the correct behaviour. */
    private static class FixedJerseyEventSink implements SseEventSink
    {
        private final SseEventSink sseEventSink;

        private FixedJerseyEventSink(SseEventSink sseEventSink)
        {
            this.sseEventSink = sseEventSink;
        }

        @Override
        public boolean isClosed()
        {
            return sseEventSink.isClosed();
        }

        @Override
        public CompletionStage<?> send(OutboundSseEvent event)
        {
            return CompletableFuture
                .supplyAsync(() -> sseEventSink.send(event)
                    .thenAcceptAsync(object -> {
                        if (object instanceof IOException)
                        {
                            throw new UncheckedIOException((IOException) object);
                        }
                    }))
                .thenComposeAsync(Function.identity());
        }

        @Override
        public void close()
        {
            sseEventSink.close();
        }
    }

    public class EventSink
    {
        private final SseEventSink sseEventSink;
        private final OutboundSseEvent heartBeatEvent;
        private final EventSource eventSource;
        private final String requestInfo;
        private Timeout heartBeat;
        private volatile boolean closed;

        private EventSink(SseEventSink sseEventSink, Sse sse, EventSource eventSource,
            HttpServletRequest request)
        {
            this.sseEventSink = new FixedJerseyEventSink(sseEventSink);
            this.heartBeatEvent = sse.newEventBuilder().comment("").build();
            this.eventSource = eventSource;

            // The request object is recycled (or at least, its contents are set to null)
            // once the request method has completed (this code will keep running after
            // that point), so we take a snapshot of its information for logging purposes.
            this.requestInfo = String.format("%s %s", request.getMethod(), request.getRequestURI());

            synchronized (activeSinks)
            {
                activeSinks.add(this);
            }
        }

        /** Send an SSE event and wait for it to complete; returns false on error or if the stream is closed */
        public boolean send(OutboundSseEvent event)
        {
            if (closed)
            {
                return false;
            }

            if (sseEventSink.isClosed())
            {
                close();
                return false;
            }

            try
            {
                logger.debug("{}: send(name='{}', id='{}', comment='{}', data={})", requestInfo,
                    event.getName(), event.getId(), event.getComment(), event.getData());
                sseEventSink.send(event).toCompletableFuture().join();
            }
            catch (CompletionException e)
            {
                logger.info("{}: error in send, assuming remote is closed: {}", requestInfo, e.getMessage());
                close();
                return false;
            }

            return true;
        }

        public boolean isClosed()
        {
            return closed;
        }

        private void sendAndRescheduleHeartbeat(Timeout ignored)
        {
            if (send(heartBeatEvent))
            {
                // We could write, reschedule heartbeat
                scheduleHeartBeat();
            }
        }

        public void close()
        {
            synchronized (this)
            {
                if (closed)
                {
                    return;
                }
                logger.info("{}: closing", requestInfo);
                closed = true;
                if (heartBeat != null)
                {
                    heartBeat.cancel();
                }
            }

            eventSource.onClose();
            sseEventSink.close();

            synchronized (activeSinks)
            {
                activeSinks.remove(this);
            }
        }

        private void scheduleHeartBeat()
        {
            synchronized (this)
            {
                if (!closed)
                {
                    heartBeat = scheduler.newTimeout(this::sendAndRescheduleHeartbeat,
                        heartBeatIntervalSeconds, TimeUnit.SECONDS);
                }
            }
        }
    }
}
