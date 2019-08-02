package com.sky.beam.exam.io.eventwatcherio.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface EventWatchOptions extends PipelineOptions {

    @Description("Text file containing events")
    @Validation.Required
    String getTextFilePath();
    void setTextFilePath(String value);


    @Description("DATAFLOW Pubsub topic for video events")
    @Validation.Required
    String getVideoEventsPubsubTopic();
    void setVideoEventsPubsubTopic(String value);

    @Description("DATAFLOW Pubsub topic for video events")
    @Validation.Required
    String getContentWatchedEventsPubsubTopic();
    void setContentWatchedEventsPubsubTopic(String value);

    @Description("Minimum Session Duration Time in seconds")
    @Validation.Required
    Long getMinSessionDurationSeconds();
    void setMinSessionDurationSeconds(Long value);

    @Description("Maximum Session Duration Time in seconds")
    @Validation.Required
    Long getMaxSessionDurationSeconds();
    void setMaxSessionDurationSeconds(Long value);

    @Description("Maximum SESSION WINDOW Duration in seconds")
    @Validation.Required
    Long getMaxWindowSessionDurationSeconds();
    void setMaxWindowSessionDurationSeconds(Long value);
}
