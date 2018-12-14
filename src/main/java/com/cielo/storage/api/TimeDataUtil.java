package com.cielo.storage.api;

import java.util.Map;

public interface TimeDataUtil {
    String LATEST = "latest_file";

    void set(String hName, Object val);

    //获取可能归档的hashMap中具体某条数据
    <T> T get(String hName, Long timestamp, Class<T> clazz) throws Exception;

    //获取可能归档的hashMap中某段数据
    <T> Map<Object, T> get(String hName, Long startTime, Long endTime, Class<T> clazz);

    //hashMap中数据归档,hashMap中key为时间戳，val为JSON对象
    void archive(String hName);

    //获取hashMap中最新归档时间
    Long getLatestFileTime(String hName);
}
