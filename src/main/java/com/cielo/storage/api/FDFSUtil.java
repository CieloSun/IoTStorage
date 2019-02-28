package com.cielo.storage.api;

import com.cielo.storage.fastdfs.FileInfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface FDFSUtil {
    String upload(String content);

    String upload(String content, String key);

    String upload(String content, Map<String, String> infos);

    String upload(String content, String key, Map<String, String> infos);

    String upload(byte[] fileContent);

    String upload(byte[] fileContent, String key);

    String upload(byte[] fileContent, Map<String, String> infos);

    String upload(byte[] fileContent, String key, Map<String, String> infos);

    <T> List<T> downloadList(String path, Class<T> clazz) throws Exception;

    <T> T downloadObject(String path, Class<T> clazz) throws Exception;

    Map downloadMap(String path) throws Exception;

    String downloadString(String path) throws Exception;

    byte[] downloadBytes(String path) throws Exception;

    void delete(String path);

    void multiDelete(Collection<String> paths);

    FileInfo info(String path) throws Exception;
}
