package com.sky.beam.exam.io.eventwatcherio.mapping;

import com.sky.beam.exam.io.eventwatcherio.event.VideoStreamEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.time.OffsetDateTime;

public class VideoStreamEventMapper extends PTransform<PCollection<String>, PCollection<VideoStreamEvent>> {

    @Override
    public PCollection<VideoStreamEvent> expand(PCollection<String> input) {
        return input.apply("Create VideoStreamEvent from File", ParDo.of(new CreateVideStreamEventFn()));
    }

    public static class CreateVideStreamEventFn extends DoFn<String, VideoStreamEvent> {

        @ProcessElement
        public void processElement(@Element String eventString, OutputReceiver<VideoStreamEvent> out) {
            OffsetDateTime startOffsetDateTime = OffsetDateTime.now();
//            VideoStreamEventParser.parseFromFileString(eventString, st);

//            StringTokenizer st = new StringTokenizer(eventString, ",");
//            int num = st.countTokens();
//            List<String> eventFields = new ArrayList<>();
//            while (st.hasMoreTokens()) {
//                eventFields.add(st.nextToken());
//            }
//
//            switch (eventFields.get(0)) {
//                case "START":
//                    OffsetDateTime startOffsetDateTime = OffsetDateTime.parse(eventFields.get(1));
//                    out.outputWithTimestamp(VideoStreamEvent.createStartEvent(eventFields.get(1), eventFields.get(2), eventFields.get(3), eventFields.get(4)),
//                            JavaTimeParser.convertOffsetDateTimeToJodaInstant(startOffsetDateTime));
//                    break;
//                case "STOP":
//                    OffsetDateTime stopOffsetDateTime = OffsetDateTime.parse(eventFields.get(1));
//                    out.outputWithTimestamp(VideoStreamEvent.createStopEvent(eventFields.get(1), eventFields.get(2)),
//                            JavaTimeParser.convertOffsetDateTimeToJodaInstant(stopOffsetDateTime));
//                    break;
//                default:
//                    throw new RuntimeException("Invalid EventType found [ " + eventFields.get(0) + " ]");
//            }
        }

    }

}
