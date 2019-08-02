package com.sky.beam.exam.io.eventwatcherio.event;

import com.sky.beam.exam.io.eventwatcherio.parser.JavaTimeParser;
import org.apache.avro.reflect.Nullable;

import java.io.Serializable;
import java.time.OffsetDateTime;

/**
 * An event that can be published whenever a user start watching content
 */
public class VideoStreamEvent implements Serializable {
    private OffsetDateTime eventTimestamp;
    private String sessionId;

    // Tells Avro that this field is nullable
    @Nullable
    private String userId;

    // Tells Avro that this field is nullable
    @Nullable
    private String contentId;

    private EventType eventType;
    private String eventTimestampStr;
    private String eventTimestampMillis;

    public VideoStreamEvent() { }

    private VideoStreamEvent(OffsetDateTime eventTimestamp, String sessionId, String userId, String contentId, EventType eventType) {
        this.eventTimestamp = eventTimestamp;
        this.sessionId = sessionId;
        this.userId = userId;
        this.contentId = contentId;
        this.eventType = eventType;
        this.eventTimestampMillis = String.valueOf(JavaTimeParser.convertOffsetDateTimeToJodaInstant(eventTimestamp).getMillis());
        this.eventTimestampStr = eventTimestamp.toString();
    }

    public static VideoStreamEvent createStartEvent(OffsetDateTime offsetDateTime,
                                                    String sessionId,
                                                    String userId,
                                                    String contentId) {
        return new VideoStreamEvent(offsetDateTime, sessionId, userId, contentId, EventType.START);
    }

    public static VideoStreamEvent createStopEvent(OffsetDateTime offsetDateTime,
                                                   String sessionId) {
        return new VideoStreamEvent(offsetDateTime, sessionId, null, null, EventType.STOP);
    }

    @Override
    public String toString() {
        return "VideoStreamEvent{" +
                "eventTimestamp=" + eventTimestamp +
                ", sessionId='" + sessionId + '\'' +
                ", userId='" + userId + '\'' +
                ", contentId='" + contentId + '\'' +
                ", eventType=" + eventType +
                ", eventTimestampStr='" + eventTimestampStr + '\'' +
                ", eventTimestampMillis='" + eventTimestampMillis + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        VideoStreamEvent that = (VideoStreamEvent) o;

        if (eventTimestamp != null ? !eventTimestamp.equals(that.eventTimestamp) : that.eventTimestamp != null)
            return false;
        if (sessionId != null ? !sessionId.equals(that.sessionId) : that.sessionId != null) return false;
        if (userId != null ? !userId.equals(that.userId) : that.userId != null) return false;
        if (contentId != null ? !contentId.equals(that.contentId) : that.contentId != null) return false;
        return eventType == that.eventType;
    }

    @Override
    public int hashCode() {
        int result = eventTimestamp != null ? eventTimestamp.hashCode() : 0;
        result = 31 * result + (sessionId != null ? sessionId.hashCode() : 0);
        result = 31 * result + (userId != null ? userId.hashCode() : 0);
        result = 31 * result + (contentId != null ? contentId.hashCode() : 0);
        result = 31 * result + (eventType != null ? eventType.hashCode() : 0);
        return result;
    }

    public OffsetDateTime getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(OffsetDateTime eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getContentId() {
        return contentId;
    }

    public void setContentId(String contentId) {
        this.contentId = contentId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getEventTimestampMillis() {
        return eventTimestampMillis;
    }

    public void setEventTimestampMillis(String eventTimestampMillis) {
        this.eventTimestampMillis = eventTimestampMillis;
    }

    public String getEventTimestampStr() {
        return eventTimestampStr;
    }

    public void setEventTimestampStr(String eventTimestampStr) {
        this.eventTimestampStr = eventTimestampStr;
    }
}
