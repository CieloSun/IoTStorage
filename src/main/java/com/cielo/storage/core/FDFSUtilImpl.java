package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.config.FDFSConfig;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.FileInfo;
import org.csource.fastdfs.StorageClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@Order(1)
class FDFSUtilImpl implements CommandLineRunner, FDFSUtil {
    @Autowired
    private FDFSConfig fdfsConfig;
    private StorageClient storageClient;

    @Override
    public void run(String... args) throws Exception {
        ClientGlobal.init(fdfsConfig.getConfigFile());
        storageClient = new StorageClient();
    }

    @Override
    public String upload(String content) throws Exception {
        return upload(content.getBytes());
    }

    @Override
    public String upload(byte[] fileContent) throws Exception {
        String[] fileIds;
        if (fdfsConfig.isAssignGroup())
            fileIds = storageClient.upload_file(fdfsConfig.getGroup(), fileContent, null, null);
        else fileIds = storageClient.upload_file(fileContent, null, null);
        return fileIds[0] + "/" + fileIds[1];
    }

    @Override
    public <T> List<T> download(String path, Class<T> clazz) throws Exception {
        return JSON.parseArray(download(path), clazz);
    }

    @Override
    public <T> T downloadObject(String path, Class<T> clazz) throws Exception {
        return JSON.parseObject(download(path), clazz);
    }

    @Override
    public <T> Map<Object, T> downloadMap(String path, Class<T> clazz) throws Exception {
        return JSON.parseObject(download(path), Map.class);
    }

    @Override
    public String download(String path) throws Exception {
        return new String(downloadBytes(path));
    }

    @Override
    public byte[] downloadBytes(String path) throws Exception {
        String[] strings = path.split("/", 2);
        return storageClient.download_file(strings[0], strings[1]);
    }

    @Override
    public Integer delete(String group, String fileId) throws Exception {
        return storageClient.delete_file(group, fileId);
    }

    @Override
    public Integer delete(String path) throws Exception {
        String[] strings = path.split("/", 2);
        return delete(strings[0], strings[1]);
    }

    @Override
    public FileInfo info(String path) throws Exception {
        String[] strings = path.split("/", 2);
        return storageClient.get_file_info(strings[0], strings[1]);
    }

    @Override
    public FileInfo info(String group, String fileId) throws Exception {
        return storageClient.get_file_info(group, fileId);
    }
}
