package com.cielo.storage.tool;

import com.alibaba.fastjson.JSON;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class JSONUtil {
    //将值为JSONString的List转变为一个JSONList
    public static String toListJSON(List<String> jsonObjects) {
        StringBuilder stringBuilder = new StringBuilder("[");
        IntStream.range(0, jsonObjects.size()).forEach(i -> {
            stringBuilder.append(jsonObjects.get(i));
            if (i < jsonObjects.size() - 1) stringBuilder.append(",");
        });
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    //将值为JSONString的Map转变为一个JSONMap
    public static String toMapJSON(Map<? extends Object, ? extends Object> jsonMap) {
        StringBuilder stringBuilder = new StringBuilder("{");
        StreamProxy.stream(jsonMap.entrySet()).map(entry -> "\"" + entry.getKey() + "\"" + ":" + entry.getValue() + ",").forEach(stringBuilder::append);
        return stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), "}").toString();
    }

    public static Map toMap(Map<? extends Object, ? extends Object> jsonMap) {
        return JSON.parseObject(toMapJSON(jsonMap), Map.class);
    }
}
