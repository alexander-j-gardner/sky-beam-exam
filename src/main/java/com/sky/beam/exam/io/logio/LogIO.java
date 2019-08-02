package com.sky.beam.exam.io.logio;


import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogIO {

    public static class Write<T> extends PTransform<PCollection<T>, PCollection<T>> {

        private static final Logger logger = LoggerFactory.getLogger(Write.class);

        private static class LogFn<T> extends DoFn<T, T>  {
            private static final Logger logger = LoggerFactory.getLogger(LogFn.class);

            @ProcessElement
            public void processElement(@Element T event, OutputReceiver<T> out) {
                logger.info("LOG IO event: {}", event.toString());
                out.output(event);
            }
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(ParDo.of(new LogFn<T>()));
        }
    }
}
