package com.sky.beam.exam.fusionbreak;

import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class FusionBreakTransform<T> extends PTransform<PCollection<T>, PCollection<T>> {

    @Override
    public PCollection<T> expand(PCollection<T> input) {
        return input.apply("AddKey", MapElements.via(new AddRandomKey<>()))
                    .apply("Fusion Break Window", Window.<KV<Long, T>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                            .withAllowedLateness(Duration.ZERO)
                            .discardingFiredPanes())
                    .apply(GroupByKey.create())
                    .apply("Remove Key", ParDo.of(new RemoveRandomKey<>()));
    }

}
