package com.sky.beam.exam.fusionbreak;

import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.util.Random;

public class AddRandomKey<T> extends SimpleFunction<T, KV<Long, T>> {

    private static final Random rand = new Random();

    @Override
    public KV<Long, T> apply(T input) { return KV.of(rand.nextLong(), input); }
}
