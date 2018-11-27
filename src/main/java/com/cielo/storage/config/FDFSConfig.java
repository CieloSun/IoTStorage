package com.cielo.storage.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("fdfs")
public class FDFSConfig {
    @Value("false")
    private boolean assignGroup;
    @Value("group1")
    private String group;
}
