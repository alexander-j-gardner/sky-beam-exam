package com.sky.beam.exam.io.eventwatcherio.mapping;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VideoStreamEventToPubsubMapper extends SimpleFunction<VideoStreamEvent, PubsubMessage> {
    private static final Logger logger = LoggerFactory.getLogger(VideoStreamEventToPubsubMapper.class);
    private static final Schema avroSchema = ReflectData.get().getSchema(VideoStreamEvent.class);
    private final AvroCoder coder = AvroCoder.of(avroSchema);

    public VideoStreamEventToPubsubMapper() { }


    @Override
    public PubsubMessage apply(VideoStreamEvent event){

        try {
            PubsubMessage message = buildMessageForEvent(event);

            logger.info("CREATED MESSAGE {}", message.getAttribute("sessionId"));

            return message;
        } catch (Exception e) {
            logger.error("FAILED TO CREATE MESSAGE for SESSION" + event.getSessionId(), e);
            return null;
        }

    }

    private PubsubMessage buildMessageForEvent(VideoStreamEvent event) throws RuntimeException {

        try {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("sessionId", event.getSessionId());
            attributes.put("eventTimestampStr", event.getEventTimestampStr());
            attributes.put("eventTimestampMillis", event.getEventTimestampMillis());
            logger.info("Event to be converter: {}", event);

            return new PubsubMessage(objToByte(event), attributes);
        } catch (Exception e) {
            throw new RuntimeException("Failure serialising TransactionEvent object ready for publishing to Pubsub: " + event.getSessionId());
        }
    }

    public static byte[] objToByte(VideoStreamEvent event) throws IOException {

        byte[] data = SerializationUtils.serialize(event);

        return data;
    }
}
