package com.sky.beam.exam.io.eventwatcherio.source;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import com.sky.beam.exam.io.eventwatcherio.parser.JavaTimeParser;
import com.sky.beam.exam.io.eventwatcherio.parser.VideoStreamEventParser;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public class VideoStreamEventFileSource  {
    public static List<VideoStreamEvent> getStreamEventsFromFile(String filePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        // Start with the current time
        OffsetDateTime dateTimeNow = OffsetDateTime.now();

        List<VideoStreamEvent> events = new ArrayList<>();
        for (String line : lines) {
            events.add(VideoStreamEventParser.parseFromFileString(line, dateTimeNow));
        }

        return events;
    }

    public static List<VideoStreamEvent> getStreamEventsFromFileForSessionId(String filePath, String sessionId, OffsetDateTime dateTimeNow) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        List<VideoStreamEvent> events = new ArrayList<>();
        for (String line : lines) {
            VideoStreamEvent event = VideoStreamEventParser.parseFromFileString(line, dateTimeNow);
            if (sessionId.equals(event.getSessionId())) {
                events.add(event);
            }
        }

        return events;
    }

    public static List<TimestampedValue<VideoStreamEvent>> getTimestampedStreamEventsFromFile(String filePath, OffsetDateTime dateTimeNow) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        List<TimestampedValue<VideoStreamEvent>> events = new ArrayList<>();
        for (String line : lines) {
            VideoStreamEvent event = VideoStreamEventParser.parseFromFileString(line, dateTimeNow);
            events.add(TimestampedValue.<VideoStreamEvent>of(event, new Instant(0))); //JavaTimeParser.convertOffsetDateTimeToJodaInstant(event.getEventTimestamp())));
        }

        return events;
    }

    public static List<TimestampedValue<VideoStreamEvent>> getTimestampedStreamEventsFromFileForSessionId(String filePath, OffsetDateTime dateTimeNow, String sessionId) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(filePath));

        List<TimestampedValue<VideoStreamEvent>> events = new ArrayList<>();
        for (String line : lines) {
            VideoStreamEvent event = VideoStreamEventParser.parseFromFileString(line, dateTimeNow);
            if (sessionId.equals(event.getSessionId())) {
                events.add(TimestampedValue.<VideoStreamEvent>of(event, JavaTimeParser.convertOffsetDateTimeToJodaInstant(event.getEventTimestamp())));
            }
        }

        return events;
    }
}
