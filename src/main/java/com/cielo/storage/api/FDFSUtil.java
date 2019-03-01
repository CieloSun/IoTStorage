package com.cielo.storage.api;

import com.cielo.storage.fastdfs.FileInfo;

import java.util.Map;

public interface FDFSUtil {
    String upload(byte[] fileContent, String key, Map<String, String> infos);

    byte[] download(String path) throws Exception;

    void delete(String path);

    FileInfo info(String path) throws Exception;
}
