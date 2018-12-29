package com.cielo.storage.api;

import com.cielo.storage.model.DataTag;

import java.util.Map;

public interface TimeDataUtil {
    void set(DataTag dataTag, Object val);

    void set(DataTag dataTag, Long timestamp, Object val);

    //获取可能归档的hashMap中具体某条数据
    <T> T get(DataTag dataTag, Long timestamp, Class<T> clazz) throws Exception;

    //获取可能归档的hashMap中某段数据
    <T> Map<Object, T> get(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz);

    //获取hashMap中最新归档时间
    Long getLatestFileTime(DataTag dataTag);

    void clear(DataTag dataTag);

    void del(DataTag dataTag, Long startTime, Long endTime);

    //hashMap中数据归档,hashMap中key为时间戳，val为JSON对象
    void archive(DataTag dataTag);
}
