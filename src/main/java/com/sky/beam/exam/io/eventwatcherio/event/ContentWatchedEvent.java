package com.sky.beam.exam.io.eventwatcherio.event;

import com.sky.beam.exam.io.eventwatcherio.emitter.CompletedSession;
import com.sky.beam.exam.io.eventwatcherio.parser.JavaTimeParser;

import java.io.Serializable;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;

/**
 * This event is published whenever both START & STOP events have been received for a session within the publication constraints
 */
public class ContentWatchedEvent implements Serializable {
    private OffsetDateTime startTimestamp;
    private String userId;
    private String contentId;
    private Duration timeWatched;
    private String eventTimestampMillis;

    public ContentWatchedEvent() {}

    private ContentWatchedEvent(OffsetDateTime startTimestamp, String userId, String contentId, Duration timeWatched) {
        this.startTimestamp = startTimestamp;
        this.userId = userId;
        this.contentId = contentId;
        this.timeWatched = timeWatched;
        this.eventTimestampMillis = String.valueOf(JavaTimeParser.convertOffsetDateTimeToJodaInstant(startTimestamp).getMillis());
    }

    public OffsetDateTime getStartTimestamp() {
        return startTimestamp;
    }


    public String getUserId() {
        return userId;
    }


    public String getContentId() {
        return contentId;
    }


    public Duration getTimeWatched() {
        return timeWatched;
    }

    public String getEventTimestampMillis() {
        return eventTimestampMillis;
    }

    public void setEventTimestampMillis(String eventTimestampMillis) {
        this.eventTimestampMillis = eventTimestampMillis;
    }

    public static ContentWatchedEvent create(OffsetDateTime startTimestamp, String userId, String contentId, Duration timeWatched) {
        return new ContentWatchedEvent(startTimestamp, userId, contentId, timeWatched);
    }

    public static ContentWatchedEvent createFrom(CompletedSession completedSession) {
        OffsetDateTime startEventOffsetDateTime = completedSession.getStartEvent().getEventTimestamp();
        OffsetDateTime stopEventOffsetDateTime = completedSession.getStopEvent().getEventTimestamp();
        Duration timeWatchedDuration = Duration.ofNanos(ChronoUnit.NANOS.between(startEventOffsetDateTime, stopEventOffsetDateTime));

        return new ContentWatchedEvent(
                startEventOffsetDateTime,
                completedSession.getStartEvent().getUserId(),
                completedSession.getStartEvent().getContentId(),
                timeWatchedDuration);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ContentWatchedEvent that = (ContentWatchedEvent) o;

        if (startTimestamp != null ? !startTimestamp.equals(that.startTimestamp) : that.startTimestamp != null)
            return false;
        if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
        if (contentId != null ? !contentId.equals(that.contentId) : that.contentId != null) return false;
        return timeWatched != null ? timeWatched.equals(that.timeWatched) : that.timeWatched == null;
    }

    @Override
    public int hashCode() {
        int result = startTimestamp != null ? startTimestamp.hashCode() : 0;
        result = 31 * result + (userId != null ? userId.hashCode() : 0);
        result = 31 * result + (contentId != null ? contentId.hashCode() : 0);
        result = 31 * result + (timeWatched != null ? timeWatched.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ContentWatchedEvent{" +
                "startTimestamp=" + startTimestamp +
                ", userId='" + userId + '\'' +
                ", contentId='" + contentId + '\'' +
                ", timeWatched=" + timeWatched +
                '}';
    }
}
