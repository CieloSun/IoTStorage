package com.cielo.storage.tool;

import java.util.Collection;
import java.util.stream.Stream;

public class StreamProxy {
    private static final int PARALLEL_SIZE = 10000;

    public static <T> Stream<T> stream(Collection<T> collection) {
        if (collection.size() < PARALLEL_SIZE) return collection.stream();
        return collection.parallelStream();
    }
}
