package com.cielo.storage.api;

import com.cielo.storage.model.InternalKey;

import java.util.Map;
import java.util.Set;

public interface TimeDataUtil {
    void set(InternalKey internalKey, Object val);

    Set<String> getTags(String type);

    //获取可能归档的hashMap中具体某条数据
    <T> T get(InternalKey internalKey, Long timestamp, Class<T> clazz) throws Exception;

    //获取可能归档的hashMap中某段数据
    <T> Map<Object, T> get(InternalKey internalKey, Long startTime, Long endTime, Class<T> clazz);

    <T> T getLatest(InternalKey internalKey, Class<T> clazz) throws Exception;

    void del(InternalKey internalKey);

    void del(InternalKey internalKey, Long startTime, Long endTime);

    void archiveJob();
}
