package eventwatcherio.emitter;

import com.sky.beam.exam.io.eventwatcherio.emitter.CompletedSession;
import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import org.junit.Assert;
import org.junit.Test;

import java.time.OffsetDateTime;

public class CompletedSessionTest {

    @Test
    public void testCompletedEvent() {
        CompletedSession completedSessionStartThenStop = new CompletedSession();
        Assert.assertFalse("Not complete", completedSessionStartThenStop.addStreamEvent(VideoStreamEvent.createStartEvent(OffsetDateTime.now(),"sessionId", "AlexG", "The Lobster")));
        Assert.assertTrue("Complete", completedSessionStartThenStop.addStreamEvent(VideoStreamEvent.createStopEvent(OffsetDateTime.now(),"sessionId")));

        CompletedSession completedSessionStopThenStart = new CompletedSession();
        Assert.assertFalse("Not Complete", completedSessionStopThenStart.addStreamEvent(VideoStreamEvent.createStopEvent(OffsetDateTime.now(),"sessionId")));
        Assert.assertTrue("Complete", completedSessionStopThenStart.addStreamEvent(VideoStreamEvent.createStartEvent(OffsetDateTime.now(),"sessionId", "AlexG", "The Lobster")));
    }

    @Test
    public void testDuplicateEvents() {
        CompletedSession completedSessionTwoStartEvents = new CompletedSession();
        Assert.assertFalse("Not complete", completedSessionTwoStartEvents.addStreamEvent(VideoStreamEvent.createStartEvent(OffsetDateTime.now(),"sessionId", "AlexG", "The Lobster")));
        Assert.assertFalse("Not complete", completedSessionTwoStartEvents.addStreamEvent(VideoStreamEvent.createStartEvent(OffsetDateTime.now(),"sessionId", "AlexG", "The Lobster")));

        CompletedSession completedSessionTwoStopEvents = new CompletedSession();
        Assert.assertFalse("Not Complete", completedSessionTwoStopEvents.addStreamEvent(VideoStreamEvent.createStopEvent(OffsetDateTime.now(),"sessionId")));
        Assert.assertFalse("Not Complete", completedSessionTwoStopEvents.addStreamEvent(VideoStreamEvent.createStopEvent(OffsetDateTime.now(),"sessionId")));
    }
}
