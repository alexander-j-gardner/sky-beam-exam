package com.sky.beam.exam.io.eventwatcherio.window.session;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyedStreamFn extends DoFn<VideoStreamEvent, KV<String, VideoStreamEvent>> {

    private static final Logger logger = LoggerFactory.getLogger(KeyedStreamFn.class);

    @ProcessElement
    public void processElement(@Element VideoStreamEvent videoStreamEvent, OutputReceiver<KV<String, VideoStreamEvent>> out) {
        logger.info("sessionid: {}, event: {}", videoStreamEvent.getSessionId(), videoStreamEvent);
        out.output(KV.of(videoStreamEvent.getSessionId(), videoStreamEvent));
    }
}
