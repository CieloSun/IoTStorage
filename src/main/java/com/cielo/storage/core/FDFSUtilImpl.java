package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.config.FDFSConfig;
import com.github.luben.zstd.Zstd;
import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.FileInfo;
import org.csource.fastdfs.StorageClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;

@Service
@Order(1)
class FDFSUtilImpl implements CommandLineRunner, FDFSUtil {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSConfig fdfsConfig;
    private StorageClient storageClient;
    private byte[] compressDictionary;

    @Override
    public void run(String... args) throws Exception {
        //初始化FDFS
        ClientGlobal.init(fdfsConfig.getConfigFile());
        storageClient = new StorageClient();
        logger.info("FDFS has init.");
        //初始化压缩字典
        if (fdfsConfig.isCompression()) {
            File file = new ClassPathResource(fdfsConfig.getDictionaryFile()).getFile();
            compressDictionary = new byte[(int) file.length()];
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            bufferedInputStream.read(compressDictionary);
            bufferedInputStream.close();
            fileInputStream.close();
            logger.info("Zstd dictionary has init.");
        }
    }

    private byte[] compress(byte[] bytes) {
        return Zstd.compressUsingDict(bytes, compressDictionary, fdfsConfig.getCompressionLevel());
    }

    private byte[] decompress(byte[] bytes) {
        int length = bytes.length;
        byte[] dst = new byte[(int) Zstd.decompressedSize(bytes)];
        Zstd.decompressUsingDict(dst, 0, bytes, 0, length, compressDictionary);
        return dst;
    }

    @Override
    public String upload(String content) throws Exception {
        return upload(content.getBytes());
    }

    @Override
    public String upload(byte[] fileContent) throws Exception {
        if (fdfsConfig.isCompression()) fileContent = compress(fileContent);
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
    public byte[] downloadBytes(String path) throws Exception {
        String[] strings = path.split("/", 2);
        byte[] bytes = storageClient.download_file(strings[0], strings[1]);
        if (fdfsConfig.isCompression()) bytes = decompress(bytes);
        return bytes;
    }

    @Override
    public String download(String path) throws Exception {
        return new String(downloadBytes(path));
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
