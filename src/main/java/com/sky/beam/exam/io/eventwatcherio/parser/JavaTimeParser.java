package com.sky.beam.exam.io.eventwatcherio.parser;

import org.joda.time.Instant;

public class JavaTimeParser {
    /**
     *
     * Converts java.time.{@link java.time.OffsetDateTime} to org.joda.time.{@link org.joda.time.Instant}
     *
     * Loss of precsion to millis in the conversion.
     *
     * @param offsetDateTime
     * @return instant in millis from offsetDateTime
     */
    public static org.joda.time.Instant convertOffsetDateTimeToJodaInstant(java.time.OffsetDateTime offsetDateTime) {
        java.time.Instant javaInstant = offsetDateTime.toInstant();

        return new org.joda.time.Instant(javaInstant.toEpochMilli());
    }
}
