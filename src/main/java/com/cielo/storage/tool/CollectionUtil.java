package com.cielo.storage.tool;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class CollectionUtil {
    public static <T> int lowerBound(List<? extends Comparable<? super T>> list, T key) {
        int low = 0;
        int high = list.size() - 1;
        while (low < high) {
            int mid = low + (high - low) / 2;
            if (list.get(mid).compareTo(key) < 0) low = mid + 1;
            else high = mid;
        }
        return low;
    }

    public static <T> Comparable<? super T> lowerBoundVal(List<? extends Comparable<? super T>> list, T key) {
        return list.get(lowerBound(list, key));
    }

    //用于流中基于类对象的某个属性去重,用于lambda filter
    public static <T> Predicate<T> distinctByKey(Function<? super T, Object> keyExtractor) {
        Map<Object, Boolean> seen = new ConcurrentHashMap<>();
        return object -> seen.putIfAbsent(keyExtractor.apply(object), Boolean.TRUE) == null;
    }

    //第一个参数的List中值会被保留的简易去重
    public static <T> List<T> addAllAndDistinctByKey(List<T> originList, List<T> newList, Function<? super T, Object> keyExtractor) {
        originList.addAll(newList);
        return originList.stream().filter(distinctByKey(keyExtractor)).collect(Collectors.toList());
    }

    public static <T> List<T> toList(Collection<T> collection) {
        return collection.parallelStream().sorted().collect(Collectors.toList());
    }

    public static List<Long> parseLongList(Collection<String> collection) {
        return collection.parallelStream().map(Long::valueOf).sorted().collect(Collectors.toList());
    }
}
