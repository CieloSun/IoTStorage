package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.api.PersistUtil;
import com.cielo.storage.config.CompressionConfig;
import com.cielo.storage.config.KVStoreConfig;
import com.cielo.storage.fastdfs.FileInfo;
import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Collection;
import java.util.Map;

@Service
@Order(3)
class PersistUtilImpl implements CommandLineRunner, PersistUtil {
    private static final String SHORT_CONTENT_LABEL = "sc@@";
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private CompressionConfig compressionConfig;
    @Autowired
    private KVStoreConfig KVStoreConfig;
    @Autowired
    private FDFSUtil fdfsUtil;
    private byte[] compressDictionary;

    private byte[] compress(byte[] bytes) {
        return Zstd.compressUsingDict(bytes, compressDictionary, compressionConfig.getCompressionLevel());
    }

    private byte[] decompress(byte[] bytes) {
        int length = bytes.length;
        byte[] dst = new byte[(int) Zstd.decompressedSize(bytes)];
        Zstd.decompressUsingDict(dst, 0, bytes, 0, length, compressDictionary);
        return dst;
    }

    @Override
    public void run(String... args) throws Exception {
        //初始化压缩字典
        if (compressionConfig.isCompression()) {
            File file = new ClassPathResource(compressionConfig.getDictionaryFile()).getFile();
            compressDictionary = new byte[(int) file.length()];
            FileInputStream fileInputStream = new FileInputStream(file);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
            bufferedInputStream.read(compressDictionary);
            bufferedInputStream.close();
            fileInputStream.close();
            logger.info("Zstd dictionary has init.");
        }
    }

    @Override
    public String upload(String content) {
        return upload(content.getBytes());
    }

    @Override
    public String upload(String content, String key) {
        return upload(content.getBytes(), key);
    }

    @Override
    public String upload(String content, Map<String, String> infos) {
        return upload(content.getBytes(), infos);
    }

    @Override
    public String upload(String content, String key, Map<String, String> infos) {
        return upload(content.getBytes(), key, infos);
    }

    @Override
    public String upload(byte[] fileContent) {
        return upload(fileContent, null, null);
    }

    @Override
    public String upload(byte[] fileContent, String key) {
        return upload(fileContent, key, null);
    }

    @Override
    public String upload(byte[] fileContent, Map<String, String> infos) {
        return upload(fileContent, null, infos);
    }

    @Override
    public String upload(byte[] fileContent, String key, Map<String, String> infos) {
        //如果是小数据不压缩直接存kv存储
        if (fileContent.length <= KVStoreConfig.getMaxSizeOfSingleValue())
            return SHORT_CONTENT_LABEL + new String(fileContent);
        if (compressionConfig.isCompression()) fileContent = compress(fileContent);
        return fdfsUtil.upload(fileContent, key, infos);
    }

    @Override
    public byte[] downloadBytes(String path) throws Exception {
        if (path.contains(SHORT_CONTENT_LABEL)) return path.replaceFirst(SHORT_CONTENT_LABEL, "").getBytes();
        byte[] bytes = fdfsUtil.download(path);
        if (compressionConfig.isCompression()) bytes = decompress(bytes);
        return bytes;
    }

    @Override
    public String downloadString(String path) throws Exception {
        return new String(downloadBytes(path));
    }

    @Override
    public Map downloadMap(String path) throws Exception {
        return JSON.parseObject(downloadString(path), Map.class);
    }

    @Override
    public void delete(String path) {
        if(path.contains(SHORT_CONTENT_LABEL)) return;
        fdfsUtil.delete(path);
    }

    @Override
    @Async
    public void multiDelete(Collection<String> paths) {
        paths.parallelStream().forEach(this::delete);
    }
}
