package com.cielo.storage.api;

import com.cielo.storage.model.DataTag;

import java.util.List;
import java.util.Map;

public interface CacheUtil {

    void set(DataTag cacheName, Object key, Object val);

    void multiSet(DataTag cacheName, Map<Object, Object> map);

    <T> T get(DataTag cacheName, Object key, Class<T> clazz);

    Object getVal(DataTag cacheName, Object key);

    Map scan(DataTag cacheName, Long startKey, Long endKey);

    <T> Map<Object, T> scan(DataTag cacheName, Long startKey, Long endKey, Class<T> clazz);

    List<String> searchCacheNames(String prefix);

    void clear(DataTag cacheName);

    void delete(DataTag cacheName, Long startKey, Long endKey);

    int size(DataTag cacheName);

}
