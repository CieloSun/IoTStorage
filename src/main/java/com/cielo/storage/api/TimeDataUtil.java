package com.cielo.storage.api;

import com.cielo.storage.model.InternalKey;

import java.util.List;
import java.util.Map;

public interface TimeDataUtil {
    void set(InternalKey internalKey, Object val);

    List<String> getInternalKeys(String primeType, String primeTag);

    List<String> getInternalKeys(String primeType, String searchType, String searchTag);

    List<String> getInternalKeys(String internalKeyPrefix);

    //获取可能归档的hashMap中具体某条数据
    <T> T get(InternalKey internalKey, Long timestamp, Class<T> clazz) throws Exception;

    //获取可能归档的hashMap中某段数据
    <T> Map<Object, T> get(InternalKey internalKey, Long startTime, Long endTime, Class<T> clazz);

    <T> T getLatest(InternalKey internalKey, Class<T> clazz) throws Exception;

    void del(InternalKey internalKey);

    void del(InternalKey internalKey, Long startTime, Long endTime);

    void archiveJob();
}
