package com.cielo.storage.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("ssdb")
public class KVStoreConfig {
    @Value("localhost")
    private String host;
    @Value("8888")
    private Integer port;
    @Value("10000")
    private Integer timeout;
    @Value("3")
    private Integer maxIdle;
    @Value("3")
    private Integer minIdle;
    @Value("5000")
    private Integer maxWait;
    @Value("100")
    private Integer numTestsPerEvictionRun;
    @Value("100000")
    private Integer softMinEvictableIdleTimeMillis;
    @Value("5000")
    private Integer timeBetweenEvictionRunsMillis;
    @Value("true")
    private boolean testOnBorrow;
    @Value("true")
    private boolean testOnReturn;
    @Value("true")
    private boolean testWhileIdle;
    @Value("true")
    private boolean lifo;
    @Value("1000")
    private int scanNumber;
    @Value("64")
    private Integer maxSizeOfSingleValue;
}
