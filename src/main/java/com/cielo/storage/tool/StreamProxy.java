package com.cielo.storage.tool;

import java.util.Collection;
import java.util.stream.Stream;

public class StreamProxy {
    private static final int PARALLEL_SIZE = 10000;

    public static <T> Stream<T> stream(Collection<T> collection) {
        return collection.size() < PARALLEL_SIZE ? collection.stream() : collection.parallelStream();
    }

    public static <T> Stream<T> stream(int parallelSize, Collection<T> collection) {
        return collection.size() < parallelSize ? collection.stream() : collection.parallelStream();
    }

    public static <T> Stream<T> parallelStream(Collection<T> collection) {
        return collection.parallelStream();
    }
}
