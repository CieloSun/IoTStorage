package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.api.PersistUtil;
import com.cielo.storage.config.CompressionConfig;
import com.cielo.storage.config.KVStoreConfig;
import com.cielo.storage.fastdfs.FileId;
import com.cielo.storage.tool.StreamProxy;
import com.github.luben.zstd.Zstd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.concurrent.CompletableFuture;

@Service
@Order(3)
class PersistUtilImpl implements CommandLineRunner, PersistUtil {
    private static final String SHORT_CONTENT_LABEL = "sc@@";
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private final CompressionConfig compressionConfig;
    private final KVStoreConfig KVStoreConfig;
    private final FDFSUtil fdfsUtil;
    private byte[] compressDictionary;

    public PersistUtilImpl(CompressionConfig compressionConfig, KVStoreConfig KVStoreConfig, FDFSUtil fdfsUtil) {
        this.compressionConfig = compressionConfig;
        this.KVStoreConfig = KVStoreConfig;
        this.fdfsUtil = fdfsUtil;
    }

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
    public CompletableFuture<String> upload(String content) throws Exception {
        return upload(content.getBytes());
    }

    @Override
    public CompletableFuture<String> upload(String content, String key) throws Exception {
        return upload(content.getBytes(), key);
    }

    @Override
    public CompletableFuture<String> upload(String content, Map<String, String> infos) throws Exception {
        return upload(content.getBytes(), infos);
    }

    @Override
    public CompletableFuture<String> upload(String content, String key, Map<String, String> infos) throws Exception {
        return upload(content.getBytes(), key, infos);
    }

    @Override
    public CompletableFuture<String> upload(byte[] fileContent) throws Exception {
        return upload(fileContent, null, null);
    }

    @Override
    public CompletableFuture<String> upload(byte[] fileContent, String key) throws Exception {
        return upload(fileContent, key, null);
    }

    @Override
    public CompletableFuture<String> upload(byte[] fileContent, Map<String, String> infos) throws Exception {
        return upload(fileContent, null, infos);
    }

    private CompletableFuture<String> uploadShort(byte[] shortContent) {
        return CompletableFuture.supplyAsync(() -> SHORT_CONTENT_LABEL + shortContent);
    }

    @Override
    public CompletableFuture<String> upload(byte[] fileContent, String key, Map<String, String> infos) throws Exception {
        //如果是小数据不压缩直接存kv存储
        if (fileContent.length <= KVStoreConfig.getMaxSizeOfSingleValue()) return uploadShort(fileContent);
        if (compressionConfig.isCompression()) fileContent = compress(fileContent);
        return fdfsUtil.upload(fileContent, key, infos).thenApply(FileId::toString);
    }

    @Override
    public CompletableFuture<byte[]> downloadBytes(String path) throws Exception {
        if (path.startsWith(SHORT_CONTENT_LABEL))
            return CompletableFuture.supplyAsync(() -> path.replaceFirst(SHORT_CONTENT_LABEL, "").getBytes());
        return fdfsUtil.download(path).thenApply(bys -> {
            if (compressionConfig.isCompression()) bys = decompress(bys);
            return bys;
        });
    }

    @Override
    public CompletableFuture<String> downloadString(String path) throws Exception {
        return downloadBytes(path).thenApply(String::new);
    }

    @Override
    public <K, V> CompletableFuture<Map<K, V>> downloadMap(String path, Class<K> keyType, Class<V> valueType) throws Exception {
        return downloadString(path).thenApply(s -> JSON.parseObject(s, new TypeReference<Map<K, V>>(keyType, valueType) {
        }));
    }

    @Override
    public void delete(String path) {
        if (path.startsWith(SHORT_CONTENT_LABEL)) return;
        fdfsUtil.delete(path);
    }

    @Override
    @Async
    public void multiDelete(Collection<String> paths) {
        StreamProxy.parallelStream(paths).forEach(this::delete);
    }
}
