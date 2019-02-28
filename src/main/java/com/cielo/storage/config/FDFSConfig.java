package com.cielo.storage.config;

import com.cielo.storage.fastdfs.TrackerServer;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties("fdfs")
public class FDFSConfig {
    @Value("false")
    private boolean assignGroup;
    @Value("group1")
    private String group;
    private List<String> trackers;
    @Value("3000")
    private Long connectTimeout;
    @Value("1000")
    private Long readTimeout;
    @Value("32")
    private Integer maxThreads;
    @Value("true")
    private boolean compression;
    @Value("3")
    private int compressionLevel;
    @Value("device.dic")
    private String dictionaryFile;
}
