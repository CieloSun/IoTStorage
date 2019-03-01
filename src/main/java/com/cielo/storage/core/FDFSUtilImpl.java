package com.cielo.storage.core;

import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.config.FDFSConfig;
import com.cielo.storage.fastdfs.FastdfsClient;
import com.cielo.storage.fastdfs.FileInfo;
import com.cielo.storage.fastdfs.FileMetadata;
import com.cielo.storage.fastdfs.TrackerServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

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

    private List<TrackerServer> getTrackers() {
        return fdfsConfig.getTrackers().parallelStream().map(s -> s.split(":", 2)).map(ss -> {
            int port = 80;
            if (ss.length == 2) port = Integer.parseInt(ss[1]);
            return new TrackerServer(ss[0], port);
        }).collect(Collectors.toList());
    }

    @Override
    public void run(String... args) {
        //初始化FDFS
        fastdfsClient = FastdfsClient.newBuilder().connectTimeout(fdfsConfig.getConnectTimeout())
                .readTimeout(fdfsConfig.getReadTimeout()).maxThreads(fdfsConfig.getMaxThreads())
                .trackers(getTrackers())
                .build();
        logger.info("FDFS has init.");
    }

    @Override
    public String upload(byte[] fileContent, String key, Map<String, String> infos) {
        try {
            if (StringUtils.isEmpty(key)) key = ".fdfs";
            return (fdfsConfig.isAssignGroup() ? infos == null ? fastdfsClient.upload(fdfsConfig.getGroup(), key, fileContent)
                    : fastdfsClient.upload(fdfsConfig.getGroup(), key, fileContent, new FileMetadata(infos)) : infos == null ? fastdfsClient.upload(key, fileContent)
                    : fastdfsClient.upload(key, fileContent, new FileMetadata(infos))).get().toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public byte[] download(String path) throws Exception {
        return fastdfsClient.download(path).get();
    }

    @Override
    public void delete(String path) {
        fastdfsClient.delete(path);
    }

    @Override
    public FileInfo info(String path) throws Exception {
        return fastdfsClient.infoGet(path).get();
    }
}
