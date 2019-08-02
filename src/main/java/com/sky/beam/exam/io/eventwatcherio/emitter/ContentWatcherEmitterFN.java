package com.sky.beam.exam.io.eventwatcherio.emitter;

import com.sky.beam.exam.io.eventwatcherio.event.ContentWatchedEvent;
import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class ContentWatcherEmitterFN extends DoFn<KV<String, Iterable<VideoStreamEvent>>, ContentWatchedEvent> {

    private static final Logger logger = LoggerFactory.getLogger(ContentWatcherEmitterFN.class);

    private ContentWatcherEmitter contentWatcherEmitter;

    public ContentWatcherEmitterFN(ContentWatcherEmitter contentWatcherEmitter) {
        this.contentWatcherEmitter = contentWatcherEmitter;
    }

    @ProcessElement
    public void processElement(@Element KV<String, Iterable<VideoStreamEvent>> sessionVideoStreamEvents, OutputReceiver<ContentWatchedEvent> out) {
        String sessionId = sessionVideoStreamEvents.getKey();
        Iterable<VideoStreamEvent> values = sessionVideoStreamEvents.getValue();
        Iterator<VideoStreamEvent> valuesIterator = values.iterator();
        VideoStreamEvent videoStreamEvent = null;
        ContentWatchedEvent contentWatchedEvent = null;

        while (valuesIterator.hasNext()) {
            videoStreamEvent = valuesIterator.next();
            contentWatchedEvent = contentWatcherEmitter.putEvent(videoStreamEvent);
            if (contentWatchedEvent != null) {
                out.output(contentWatchedEvent);
                logger.info("Emitted: SessionId: {}, Content: {}", sessionId, contentWatchedEvent);
            }
        }
    }
}