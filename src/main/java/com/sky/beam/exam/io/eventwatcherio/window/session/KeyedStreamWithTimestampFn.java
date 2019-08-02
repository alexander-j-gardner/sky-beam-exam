package com.sky.beam.exam.io.eventwatcherio.window.session;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import com.sky.beam.exam.io.eventwatcherio.parser.JavaTimeParser;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class KeyedStreamWithTimestampFn extends DoFn<VideoStreamEvent, KV<String, VideoStreamEvent>> {

    @ProcessElement
    public void processElement(@Element VideoStreamEvent videoStreamEvent, OutputReceiver<KV<String, VideoStreamEvent>> out) {
        out.outputWithTimestamp(KV.of(videoStreamEvent.getSessionId(), videoStreamEvent),
                JavaTimeParser.convertOffsetDateTimeToJodaInstant(videoStreamEvent.getEventTimestamp()));
    }
}
