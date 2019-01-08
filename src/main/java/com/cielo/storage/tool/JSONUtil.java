package com.cielo.storage.tool;

import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class JSONUtil {
    //将值为JSONString的List转变为一个JSONList
    public static String toList(List<String> jsonObjects) {
        StringBuilder stringBuilder = new StringBuilder("[");
        IntStream.range(0, jsonObjects.size()).forEach(i -> {
            stringBuilder.append(jsonObjects.get(i));
            if (i < jsonObjects.size() - 1) stringBuilder.append(",");
        });
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    //将值为JSONString的Map转变为一个JSONMap
    public static String toMap(Map<String, String> jsonMap) {
        StringBuilder stringBuilder = new StringBuilder("{");
        jsonMap.entrySet().parallelStream().map(entry -> "\"" + entry.getKey() + "\"" + ":" + entry.getValue() + ",").forEach(stringBuilder::append);
        return stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), "}").toString();
    }

    public static <T> Map<Object, T> toMap(Map<String, String> jsonMap, Class<T> clazz) {
        return JSON.parseObject(toMap(jsonMap), Map.class);
    }

    //将值为JSONListString的List合并为同一个列表的JSONList
    public static String mergeList(List<String> jsonLists) {
        return toList(jsonLists).replace("],[", ",").replace("[[", "[").replace("]]", "]");
    }
}
