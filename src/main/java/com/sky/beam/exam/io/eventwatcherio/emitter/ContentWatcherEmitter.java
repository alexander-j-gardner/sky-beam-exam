package com.sky.beam.exam.io.eventwatcherio.emitter;

import com.sky.beam.exam.io.eventwatcherio.event.ContentWatchedEvent;
import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import com.sky.beam.exam.io.eventwatcherio.event.validation.ContentWatchedEventValidator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Creates a {@link ContentWatchedEvent} when both START & STOP events for the same sessionID have been received.
 */
public class ContentWatcherEmitter implements Serializable {
    private final Map<String, CompletedSession> streamEventsMap = new HashMap<>();
    private final ContentWatchedEventValidator contentWatchedEventValidator;

    public ContentWatcherEmitter(ContentWatchedEventValidator contentWatchedEventValidator) {
        this.contentWatchedEventValidator = contentWatchedEventValidator;
    }

    public ContentWatchedEvent putEvent(VideoStreamEvent videoStreamEvent) {
        CompletedSession cachedCompletedSession = streamEventsMap.get(videoStreamEvent.getSessionId());
        if (cachedCompletedSession == null) {
            CompletedSession newCompletedSession = new CompletedSession();
            newCompletedSession.addStreamEvent(videoStreamEvent);
            streamEventsMap.put(videoStreamEvent.getSessionId(), newCompletedSession);
            return null;
        }

        if (cachedCompletedSession.addStreamEvent(videoStreamEvent)) {
            ContentWatchedEvent contentWatchedEvent = ContentWatchedEvent.createFrom(cachedCompletedSession);
            streamEventsMap.remove(videoStreamEvent.getSessionId());
            if (contentWatchedEventValidator.isValid(contentWatchedEvent)) {
                return contentWatchedEvent;
            }
        }

        return null;
    }

    public boolean containsCompletedSession(String sessionId) {
        CompletedSession completedSession = streamEventsMap.get(sessionId);

        return (completedSession != null) ? completedSession.isCompleted() : false;
    }

    public boolean containsSession(String sessionId) {
        CompletedSession completedSession = streamEventsMap.get(sessionId);

        return (completedSession != null) ? true : false;
    }
}
