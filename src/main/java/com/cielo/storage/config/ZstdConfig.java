package com.cielo.storage.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("zstd")
public class ZstdConfig {
    @Value("3")
    private int level;
    @Value("jsonDic")
    private String dictionaryFile;
}
