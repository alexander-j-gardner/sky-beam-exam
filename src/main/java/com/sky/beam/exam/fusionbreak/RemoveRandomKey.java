package com.sky.beam.exam.fusionbreak;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class RemoveRandomKey<T> extends DoFn<KV<Long, Iterable<T>>, T> {

    @ProcessElement
    public void process(@Element KV<Long, Iterable<T>> input, OutputReceiver<T> output) {
        for (T inputEvent: input.getValue()) {
            output.output(inputEvent);
        }
    }
}
