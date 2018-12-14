package com.cielo.storage.api;

import org.csource.fastdfs.FileInfo;

import java.util.List;
import java.util.Map;

public interface FDFSUtil {
    String upload(String content) throws Exception;

    String upload(byte[] fileContent) throws Exception;

    <T> List<T> download(String path, Class<T> clazz) throws Exception;

    <T> T downloadObject(String path, Class<T> clazz) throws Exception;

    <T> Map<Object, T> downloadMap(String path, Class<T> clazz) throws Exception;

    String download(String path) throws Exception;

    byte[] downloadBytes(String path) throws Exception;

    Integer delete(String group, String fileId) throws Exception;

    Integer delete(String path) throws Exception;

    FileInfo info(String path) throws Exception;

    FileInfo info(String group, String fileId) throws Exception;
}
