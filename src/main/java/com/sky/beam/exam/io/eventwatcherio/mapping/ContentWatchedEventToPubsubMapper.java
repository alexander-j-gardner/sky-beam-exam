package com.sky.beam.exam.io.eventwatcherio.mapping;

import com.sky.beam.exam.io.eventwatcherio.event.ContentWatchedEvent;
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

public class ContentWatchedEventToPubsubMapper extends SimpleFunction<ContentWatchedEvent, PubsubMessage> {
    private static final Logger logger = LoggerFactory.getLogger(ContentWatchedEventToPubsubMapper.class);
    private static final Schema avroSchema = ReflectData.get().getSchema(ContentWatchedEvent.class);
    private final AvroCoder coder = AvroCoder.of(avroSchema);

    public ContentWatchedEventToPubsubMapper() { }


    @Override
    public PubsubMessage apply(ContentWatchedEvent event){

        try {
            PubsubMessage message = buildMessageForEvent(event);

            logger.info("CREATED MESSAGE {}", message.getAttribute("sessionId"));

            return message;
        } catch (Exception e) {
            logger.error("FAILED TO CREATE MESSAGE: " + event, e);
            return null;
        }

    }

    private PubsubMessage buildMessageForEvent(ContentWatchedEvent event) throws RuntimeException {

        try {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("eventTimestampMillis", event.getEventTimestampMillis());
            logger.info("Event to be converted to pubsub msg: {}", event);

            return new PubsubMessage(objToByte(event), attributes);
        } catch (Exception e) {
            throw new RuntimeException("Failure serialising object ready for publishing to Pubsub: " + event);
        }
    }

    public static byte[] objToByte(ContentWatchedEvent event) throws IOException {

        byte[] data = SerializationUtils.serialize(event);

        return data;
    }
}
