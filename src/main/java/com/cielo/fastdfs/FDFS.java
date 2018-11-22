package com.cielo.fastdfs;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.FileInfo;
import org.csource.fastdfs.StorageClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.IntStream;

@Service
@Order(3)
@Configuration
@ConfigurationProperties("fdfs")
@Data
public class FDFS implements CommandLineRunner {
    private StorageClient storageClient;
    @Value("false")
    private boolean assignGroup;
    @Value("group1")
    private String group;

    @Override
    public void run(String... args) throws Exception {
        ClientGlobal.init("fdfs_client.conf");
        storageClient = new StorageClient();
    }

    public String upload(String content) throws Exception {
        return upload(content.getBytes());
    }

    public String upload(byte[] fileContent) throws Exception {
        String[] fileIds;
        if (assignGroup) fileIds = storageClient.upload_file(group, fileContent, null, null);
        else fileIds = storageClient.upload_file(fileContent, null, null);
        return fileIds[0] + "/" + fileIds[1];
    }

    public <T> List<T> downloadObjects(String path, Class<T> clazz) throws Exception {
        return JSON.parseArray(download(path), clazz);
    }

    public String download(String path) throws Exception {
        return new String(downloadBytes(path));
    }

    public byte[] downloadBytes(String path) throws Exception {
        String[] strings = path.split("/", 2);
        return storageClient.download_file(strings[0], strings[1]);
    }

    public Integer delete(String group, String fileId) throws Exception {
        return storageClient.delete_file(group, fileId);
    }

    public Integer delete(String path) throws Exception {
        String[] strings = path.split("/", 2);
        return delete(strings[0], strings[1]);
    }

    public FileInfo info(String path) throws Exception {
        String[] strings = path.split("/", 2);
        return storageClient.get_file_info(strings[0], strings[1]);
    }

    public FileInfo info(String group, String fileId) throws Exception {
        return storageClient.get_file_info(group, fileId);
    }
}
