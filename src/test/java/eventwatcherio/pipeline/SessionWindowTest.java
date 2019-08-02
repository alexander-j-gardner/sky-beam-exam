package eventwatcherio.pipeline;

import com.sky.beam.exam.io.eventwatcherio.emitter.ContentWatcherEmitter;
import com.sky.beam.exam.io.eventwatcherio.event.ContentWatchedEvent;
import com.sky.beam.exam.io.eventwatcherio.emitter.ContentWatcherEmitterFN;
import com.sky.beam.exam.io.eventwatcherio.event.validation.ContentWatchedEventValidator;
import com.sky.beam.exam.io.eventwatcherio.parser.JavaTimeParser;
import com.sky.beam.exam.io.eventwatcherio.window.session.KeyedStreamFn;
import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import com.sky.beam.exam.io.eventwatcherio.options.EventWatchOptions;
import com.sky.beam.exam.io.eventwatcherio.source.VideoStreamEventFileSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Iterator;

/**
 * Test to prove that, in the absence of PubSub as a Source, that we inject {@link VideoStreamEvent}'s into the pipeline with event timestamps,
 * sourced from a {@link java.io.File}, to allow the Session windows to function correctly and be testable.
 */
public class SessionWindowTest implements Serializable {

    private final transient EventWatchOptions options = PipelineOptionsFactory.fromArgs(
            "--textFilePath=/Users/alexandergardner/Documents/github-projects/sky-beam-exam/src/main/resources/video-stream-events.txt",
            "--videoEventsPubsubTopic=DummyTopic",
            "--contentWatchedEventsPubsubTopic=DummyTopic",
            "--minSessionDurationSeconds=70",
            "--maxSessionDurationSeconds=100",
            "--maxWindowSessionDurationSeconds=70"
    )
            .withValidation().as(EventWatchOptions.class);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.fromOptions(options);

    @Test
    public void testEmittedEvents() throws IOException {
        PCollection<ContentWatchedEvent> contentWatchedEvents =
                pipeline.apply(Create.of(VideoStreamEventFileSource.getStreamEventsFromFile(options.getTextFilePath())))
                .apply("Add timestamps", WithTimestamps.<VideoStreamEvent>of((VideoStreamEvent event) ->
                        new Instant(JavaTimeParser.convertOffsetDateTimeToJodaInstant(event.getEventTimestamp()))))
                .apply("Create key (sessionId) value (VideoStreamEvent)", ParDo.of(new KeyedStreamFn()))
                .apply(Window.<KV<String, VideoStreamEvent>>into(Sessions.withGapDuration(org.joda.time.Duration.standardSeconds(options.getMaxWindowSessionDurationSeconds()))))
                .apply(GroupByKey.create())
                .apply("Emit ContentWatchedEvents", ParDo.of(new ContentWatcherEmitterFN(
                        new ContentWatcherEmitter(new ContentWatchedEventValidator(options.getMinSessionDurationSeconds(),
                                                                                   options.getMaxSessionDurationSeconds())))));

        PAssert.thatSingleton(contentWatchedEvents).satisfies(new SerializableFunction<ContentWatchedEvent, Void>() {
            @Override
            public Void apply(ContentWatchedEvent input) {
                    Assert.assertEquals("AlexG", input.getUserId());
                    Assert.assertEquals("Game Of Thrones 8", input.getContentId());
                    Assert.assertEquals(Duration.ofSeconds(90L), input.getTimeWatched());

                return null;
            }
        });

        pipeline.run();
    }


    public static <T> Iterable<T>
    getIterableFromIterator(Iterator<T> iterator)
    {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator()
            {
                return iterator;
            }
        };
    }
}
