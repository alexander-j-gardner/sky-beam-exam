package com.sky.beam.exam.io.eventwatcherio.pipeline;

import com.sky.beam.exam.io.eventwatcherio.emitter.ContentWatcherEmitter;
import com.sky.beam.exam.io.eventwatcherio.emitter.ContentWatcherEmitterFN;
import com.sky.beam.exam.io.eventwatcherio.event.ContentWatchedEvent;
import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import com.sky.beam.exam.io.eventwatcherio.event.validation.ContentWatchedEventValidator;
import com.sky.beam.exam.io.eventwatcherio.mapping.PubsubToVideoStreamEventMapper;
import com.sky.beam.exam.io.eventwatcherio.options.EventWatchOptions;
import com.sky.beam.exam.io.eventwatcherio.window.session.KeyedStreamFn;
import com.sky.beam.exam.io.logio.LogIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;


public class VideoStreamEventsConsumer {

    private static final Logger logger = LoggerFactory.getLogger(VideoStreamEventsConsumer.class);

    public static void main(String[] args) throws IOException {
        prepareFileSourcedPipeline(args);
    }

    private static void prepareFileSourcedPipeline(String[] args) throws IOException {
        EventWatchOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(EventWatchOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read video events from Pubsub",
                PubsubIO.readMessagesWithAttributes()
                        .fromTopic(options.getPubsubTopic())  //"projects/crucial-module-223618/topics/events-topic")  c("projects/sky-project/topics/video-events")
                    .withTimestampAttribute("eventTimestampMillis"))
                .apply("Map Pubsub msg to VideoStreamEvent", MapElements.via(new PubsubToVideoStreamEventMapper()))
                .apply("Add timestamps", WithTimestamps.<VideoStreamEvent>of((VideoStreamEvent event) -> {
                            Instant javaTimeInstant = event.getEventTimestamp().toInstant();
                            return new org.joda.time.Instant(javaTimeInstant.toEpochMilli());
                        } )) //.withAllowedTimestampSkew(Duration.standardSeconds(100)))
                .apply("Create key (sessionId) value (VideoStreamEvent)", ParDo.of(new KeyedStreamFn()))
                .apply(Window.<KV<String, VideoStreamEvent>>into(Sessions.withGapDuration(Duration.standardSeconds(options.getMaxWindowSessionDurationSeconds()))))
                .apply(GroupByKey.create())
                .apply("Emit ContentWatchedEvents", ParDo.of(new ContentWatcherEmitterFN(
                        new ContentWatcherEmitter(new ContentWatchedEventValidator(options.getMinSessionDurationSeconds(), options.getMaxSessionDurationSeconds())))))
                .apply(new LogIO.Write<ContentWatchedEvent>());

         pipeline.run();
    }


}
