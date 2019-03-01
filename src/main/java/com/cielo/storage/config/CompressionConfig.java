package com.cielo.storage.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("compression")
public class CompressionConfig {
    @Value("true")
    private boolean compression;
    @Value("3")
    private int compressionLevel;
    @Value("device.dic")
    private String dictionaryFile;
}
