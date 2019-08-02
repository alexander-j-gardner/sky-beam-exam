package com.sky.beam.exam.io.eventwatcherio.pipeline;

import com.sky.beam.exam.io.eventwatcherio.mapping.VideoStreamEventToPubsubMapper;
import com.sky.beam.exam.io.eventwatcherio.options.EventWatchOptions;
import com.sky.beam.exam.io.eventwatcherio.source.VideoStreamEventFileSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class VideoStreamEventsPublisher {

    private static final Logger logger = LoggerFactory.getLogger(VideoStreamEventsPublisher.class);

    public static void main(String[] args) throws IOException {
        prepareFileSourcedPipeline(args);
    }

    private static void prepareFileSourcedPipeline(String[] args) throws IOException {
        EventWatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(EventWatchOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        logger.info("Google credentials: {}", System.getenv("GOOGLE_APPLICATION_CREDENTIALS"));
        pipeline.apply(Create.of(VideoStreamEventFileSource.getStreamEventsFromFile(options.getTextFilePath())))
                .apply("Prepare message", MapElements.via(new VideoStreamEventToPubsubMapper()))
                .apply("publish message",
                        PubsubIO.writeMessages()
                                .to(options.getVideoEventsPubsubTopic()) //"projects/crucial-module-223618/topics/events-topic")
//                                .to("projects/sky-project/topics/video-events")
                                .withTimestampAttribute("eventTimestampMillis")
                );
         pipeline.run();
    }


}
