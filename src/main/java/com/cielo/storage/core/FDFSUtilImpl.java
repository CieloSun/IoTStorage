package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.config.FDFSConfig;
import com.cielo.storage.fastdfs.FastdfsClient;
import com.cielo.storage.fastdfs.FileInfo;
import com.cielo.storage.fastdfs.TrackerServer;
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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Order(1)
class FDFSUtilImpl implements CommandLineRunner, FDFSUtil {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSConfig fdfsConfig;
    private FastdfsClient fastdfsClient;
    private byte[] compressDictionary;

    private List<TrackerServer> getTrackers() {
        return fdfsConfig.getTrackers().parallelStream().map(s -> s.split(":", 2)).map(ss -> {
            int port = 80;
            if (ss.length == 2) port = Integer.parseInt(ss[1]);
            return new TrackerServer(ss[0], port);
        }).collect(Collectors.toList());
    }

    @Override
    public void run(String... args) throws Exception {
        //初始化FDFS
        fastdfsClient = FastdfsClient.newBuilder().connectTimeout(fdfsConfig.getConnectTimeout())
                .readTimeout(fdfsConfig.getReadTimeout()).maxThreads(fdfsConfig.getMaxThreads())
                .trackers(getTrackers())
                .build();
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
        if (fdfsConfig.isAssignGroup())
            return fastdfsClient.upload(fdfsConfig.getGroup(), "tmp", fileContent).get().toString();
        return fastdfsClient.upload("tmp", fileContent).get().toString();
    }

    @Override
    public <T> List<T> downloadList(String path, Class<T> clazz) throws Exception {
        return JSON.parseArray(downloadString(path), clazz);
    }

    @Override
    public byte[] downloadBytes(String path) throws Exception {
        byte[] bytes = fastdfsClient.download(path).get();
        if (fdfsConfig.isCompression()) bytes = decompress(bytes);
        return bytes;
    }

    @Override
    public String downloadString(String path) throws Exception {
        return new String(downloadBytes(path));
    }

    @Override
    public <T> T downloadObject(String path, Class<T> clazz) throws Exception {
        return JSON.parseObject(downloadString(path), clazz);
    }

    @Override
    public <T> Map<Object, T> downloadMap(String path, Class<T> clazz) throws Exception {
        return JSON.parseObject(downloadString(path), Map.class);
    }

    @Override
    public void delete(String path) {
        fastdfsClient.delete(path);
    }

    @Override
    @Async
    public void multiDelete(Collection<String> paths) {
        paths.parallelStream().forEach(this::delete);
    }

    @Override
    public FileInfo info(String path) throws Exception {
        return fastdfsClient.infoGet(path).get();
    }
}
