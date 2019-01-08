package com.cielo.storage.api;

import com.cielo.storage.fastdfs.FileInfo;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface FDFSUtil {
    String upload(String content) throws Exception;

    String upload(byte[] fileContent) throws Exception;

    <T> List<T> downloadList(String path, Class<T> clazz) throws Exception;

    <T> T downloadObject(String path, Class<T> clazz) throws Exception;

    <T> Map<Object, T> downloadMap(String path, Class<T> clazz) throws Exception;

    String downloadString(String path) throws Exception;

    byte[] downloadBytes(String path) throws Exception;

    void delete(String path);

    void multiDelete(Collection<String> paths);

    FileInfo info(String path) throws Exception;
}
