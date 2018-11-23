package com.cielo.storage;

import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.stream.IntStream;

public class JSONUtil {
    public static String merge(List<String> jsonObjects) {
        StringBuilder stringBuilder = new StringBuilder("[");
        IntStream.range(0, jsonObjects.size()).forEach(i -> {
            stringBuilder.append(jsonObjects.get(i));
            if (i < jsonObjects.size() - 1) stringBuilder.append(",");
        });
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    public static <T> List<T> merge(List<String> jsonObjects, Class<T> clazz) {
        return JSON.parseArray(merge(jsonObjects), clazz);
    }

    public static String mergeList(List<String> jsonLists) {
        return merge(jsonLists).replace("],[", ",").replace("[[", "[").replace("]]", "]");
    }

    public static <T> List<T> mergeList(List<String> jsonLists, Class<T> clazz) {
        return JSON.parseArray(mergeList(jsonLists), clazz);
    }
}
