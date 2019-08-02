package com.sky.beam.exam.io.eventwatcherio.mapping;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;


public class PubsubToVideoStreamEventMapper extends SimpleFunction<PubsubMessage, VideoStreamEvent> {

    private static final Logger logger = LoggerFactory.getLogger(PubsubToVideoStreamEventMapper.class);

    public PubsubToVideoStreamEventMapper() { }


    @Override
    public VideoStreamEvent apply(PubsubMessage message){
        byte[] payload = message.getPayload();
        VideoStreamEvent event = SerializationUtils.deserialize(payload);
        event.setEventTimestamp(OffsetDateTime.parse(message.getAttribute("eventTimestampStr")));

        return event;
    }

}
