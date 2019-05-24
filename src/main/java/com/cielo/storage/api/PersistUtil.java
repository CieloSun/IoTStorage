package com.cielo.storage.api;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface PersistUtil {
    CompletableFuture<String> upload(String content) throws Exception;

    CompletableFuture<String> upload(String content, String key) throws Exception;

    CompletableFuture<String> upload(String content, Map<String, String> infos) throws Exception;

    CompletableFuture<String> upload(String content, String key, Map<String, String> infos) throws Exception;

    CompletableFuture<String> upload(byte[] fileContent) throws Exception;

    CompletableFuture<String> upload(byte[] fileContent, String key) throws Exception;

    CompletableFuture<String> upload(byte[] fileContent, Map<String, String> infos) throws Exception;

    CompletableFuture<String> upload(byte[] fileContent, String key, Map<String, String> infos) throws Exception;

    <K, V> CompletableFuture<Map<K, V>> downloadMap(String path, Class<K> keyType, Class<V> valueType) throws Exception;

    CompletableFuture<String> downloadString(String path) throws Exception;

    CompletableFuture<byte[]> downloadBytes(String path) throws Exception;

    void delete(String path);

    void multiDelete(Collection<String> paths);
}
