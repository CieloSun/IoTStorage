package com.cielo.storage.api;

import java.util.Collection;
import java.util.Map;

public interface PersistUtil {
    String upload(String content);

    String upload(String content, String key);

    String upload(String content, Map<String, String> infos);

    String upload(String content, String key, Map<String, String> infos);

    String upload(byte[] fileContent);

    String upload(byte[] fileContent, String key);

    String upload(byte[] fileContent, Map<String, String> infos);

    String upload(byte[] fileContent, String key, Map<String, String> infos);

    <K, V> Map<K, V> downloadMap(String path, Class<K> keyType, Class<V> valueType) throws Exception;

    String downloadString(String path) throws Exception;

    byte[] downloadBytes(String path) throws Exception;

    void delete(String path);

    void multiDelete(Collection<String> paths);
}
