package com.sky.beam.exam.io.eventwatcherio.parser;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class VideoStreamEventParser {

    private static final Logger logger = LoggerFactory.getLogger(VideoStreamEventParser.class);

    public static VideoStreamEvent parseFromFileString(String videoEventString, OffsetDateTime startDateTime) {
        StringTokenizer st = new StringTokenizer(videoEventString, ",");
        int numTokens = st.countTokens();
        List<String> eventFields = new ArrayList<>();
        while (st.hasMoreTokens()) {
            eventFields.add(st.nextToken());
        }

        switch (eventFields.get(0)) {
            case "START":
                if (numTokens != 5) {
                    throw new RuntimeException("START events must have 5 values not [ " + numTokens + " ]");
                }

                OffsetDateTime adjusted = startDateTime.plusSeconds(Long.parseLong(eventFields.get(4)));
                return VideoStreamEvent.createStartEvent(adjusted, eventFields.get(1), eventFields.get(2), eventFields.get(3));
            case "STOP":
                if (numTokens != 3) {
                    throw new RuntimeException("STOP events must have 3 values not [ " + numTokens + " ]");
                }
                return VideoStreamEvent.createStopEvent(startDateTime.plusSeconds(Long.parseLong(eventFields.get(2))), eventFields.get(1));
            default:
                throw new RuntimeException("Invalid EventType found [ " + eventFields.get(0) + " ]");
        }
    }
}
