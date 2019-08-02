package com.sky.beam.exam.io.eventwatcherio.emitter;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;

import java.io.Serializable;

/**
 * Represents a completed session whereby a start and stop event for the same sessionId have been received, in any order, to make the session complete.
 */
public class CompletedSession implements Serializable {
    private VideoStreamEvent startEvent;
    private VideoStreamEvent stopEvent;

    public boolean addStreamEvent(VideoStreamEvent videoStreamEvent) {
        switch(videoStreamEvent.getEventType()) {
            case START:
                // reject duplicate event
                if (startEvent == null) {
                    startEvent = videoStreamEvent;
                }
                break;
            case STOP:
                // reject duplicate event
                if (stopEvent == null) {
                    stopEvent = videoStreamEvent;
                }
                break;
            default:
                throw new RuntimeException("Unsupported EventType [ " + videoStreamEvent.getEventType() + " ]");
        }

        return isCompleted();
    }

    public VideoStreamEvent getStartEvent() {
        return startEvent;
    }

    public VideoStreamEvent getStopEvent() {
        return stopEvent;
    }

    public boolean isCompleted() {
        return (startEvent != null & stopEvent != null) ? true : false;
    }
}
