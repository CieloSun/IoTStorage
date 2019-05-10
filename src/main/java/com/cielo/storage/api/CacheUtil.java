package com.cielo.storage.api;

import com.cielo.storage.model.InternalKey;

import java.util.List;
import java.util.Map;

public interface CacheUtil {

    void set(InternalKey cacheName, Object key, Object val);

    void multiSet(InternalKey cacheName, Map<Object, Object> map);

    <T> T get(InternalKey cacheName, Object key, Class<T> clazz);

    Object getVal(InternalKey cacheName, Object key);

    Map scan(InternalKey cacheName, Long startKey, Long endKey);

    <T> Map<Object, T> scan(InternalKey cacheName, Long startKey, Long endKey, Class<T> clazz);

    List<String> searchCacheNames(String prefix);

    List<InternalKey> allInternalKeys();

    void clear(InternalKey cacheName);

    void delete(InternalKey cacheName, Long startKey, Long endKey);

    int size(InternalKey cacheName);

}
