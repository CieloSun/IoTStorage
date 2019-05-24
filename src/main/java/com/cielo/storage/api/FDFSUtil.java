package com.cielo.storage.api;

import com.cielo.storage.fastdfs.FileId;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface FDFSUtil {
    CompletableFuture<FileId> upload(byte[] fileContent, String key, Map<String, String> infos);

    CompletableFuture<byte[]> download(String path) throws Exception;

    void delete(String path);
}
