package com.cielo.storage.api;

import com.cielo.storage.model.DataTag;

import java.util.Map;

public interface TimeDataUtil {
    void set(DataTag dataTag, Object val);

    //获取可能归档的hashMap中具体某条数据
    <T> T get(DataTag dataTag, Long timestamp, Class<T> clazz) throws Exception;

    //获取可能归档的hashMap中某段数据
    <T> Map<Object, T> get(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz);

    <T> T getLatest(DataTag dataTag, Class<T> clazz) throws Exception;

    void del(DataTag dataTag);

    void del(DataTag dataTag, Long startTime, Long endTime);

    void archiveJob();

    void clearJob();
}
