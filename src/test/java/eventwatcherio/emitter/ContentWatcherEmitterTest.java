package eventwatcherio.emitter;

import com.sky.beam.exam.io.eventwatcherio.emitter.ContentWatcherEmitter;
import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import com.sky.beam.exam.io.eventwatcherio.event.validation.ContentWatchedEventValidator;
import org.junit.Assert;
import org.junit.Test;

import java.time.OffsetDateTime;

public class ContentWatcherEmitterTest {

    @Test
    public void testEventNotPublished() {
        ContentWatcherEmitter emitter = new ContentWatcherEmitter(new ContentWatchedEventValidator(70L, 100L));

        OffsetDateTime startTime = OffsetDateTime.now();

        Assert.assertNull("Event not published", emitter.putEvent(VideoStreamEvent.createStartEvent(startTime,"sessionId", "AlexG", "Seven")));
        Assert.assertFalse("Session is not complete", emitter.containsCompletedSession("sessionId"));
        Assert.assertTrue("Session is in emitter", emitter.containsSession("sessionId"));

        Assert.assertNull("Event not published", emitter.putEvent(VideoStreamEvent.createStopEvent(startTime.plusSeconds(10),"sessionId")));

        Assert.assertFalse("Session must be removed from emitter", emitter.containsCompletedSession("sessionId"));
        Assert.assertFalse("Session is in emitter", emitter.containsSession("sessionId"));
    }

    @Test
    public void testEventPublished() {
        ContentWatcherEmitter emitter = new ContentWatcherEmitter(new ContentWatchedEventValidator(70L, 100L));

        OffsetDateTime startTime = OffsetDateTime.now();

        Assert.assertNull("Event not published", emitter.putEvent(VideoStreamEvent.createStartEvent(startTime,"sessionId", "AlexG", "Seven")));
        Assert.assertNotNull("Event published", emitter.putEvent(VideoStreamEvent.createStopEvent(startTime.plusSeconds(71),"sessionId")));
    }

    @Test
    public void testExamCriteria() {
        ContentWatcherEmitter emitter = new ContentWatcherEmitter(new ContentWatchedEventValidator(600L, 86400L));

        OffsetDateTime startTime = OffsetDateTime.now();

        Assert.assertNull("Event not published", emitter.putEvent(VideoStreamEvent.createStartEvent(startTime,"sessionId", "AlexG", "Seven")));
        Assert.assertNull("Event published", emitter.putEvent(VideoStreamEvent.createStopEvent(startTime.plusSeconds(71),"sessionId")));

        startTime = OffsetDateTime.now();

        Assert.assertNull("Event not published", emitter.putEvent(VideoStreamEvent.createStartEvent(startTime,"sessionId", "AlexG", "Seven")));
        Assert.assertNull("Event published", emitter.putEvent(VideoStreamEvent.createStopEvent(startTime.plusSeconds(71),"sessionId")));
    }
}
