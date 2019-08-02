package com.sky.beam.exam.io.eventwatcherio.event.validation;

import com.sky.beam.exam.io.eventwatcherio.event.ContentWatchedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class ContentWatchedEventValidator implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ContentWatchedEventValidator.class);
    private final long MIN_SESSION_DURATION_SECONDS;
    private final long MAX_SESSION_DURATION_SECONDS;

    public ContentWatchedEventValidator(long MIN_SESSION_DURATION_SECONDS, long MAX_SESSION_DURATION_SECONDS) {
        this.MIN_SESSION_DURATION_SECONDS = MIN_SESSION_DURATION_SECONDS;
        this.MAX_SESSION_DURATION_SECONDS = MAX_SESSION_DURATION_SECONDS;
    }

    public boolean isValid(ContentWatchedEvent contentWatchedEvent) {
        long durationSecs = contentWatchedEvent.getTimeWatched().getSeconds();
        logger.info("contentId: {}, durationSecs: {}", contentWatchedEvent.getContentId(), durationSecs);
        if ((durationSecs > MIN_SESSION_DURATION_SECONDS) & (durationSecs < MAX_SESSION_DURATION_SECONDS)) {
            return true;
        }

        return false;
    }
}
