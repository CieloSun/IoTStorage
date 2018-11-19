package com.cielo.ssdb;

import lombok.Data;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Order(1)
@Data
@Configuration
@ConfigurationProperties("ssdb-local")
public class SSDBLocal extends SSDBBase implements CommandLineRunner {
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

    @Override
    @Async
    public void run(String... args) {
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();

        genericObjectPoolConfig.setMaxIdle(maxIdle);
        genericObjectPoolConfig.setMinIdle(minIdle);
        genericObjectPoolConfig.setMaxWaitMillis(maxWait);
        genericObjectPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        genericObjectPoolConfig.setTestOnBorrow(testOnBorrow);
        genericObjectPoolConfig.setTestOnReturn(testOnReturn);
        genericObjectPoolConfig.setTestWhileIdle(testWhileIdle);
        genericObjectPoolConfig.setLifo(lifo);

        ssdb = SSDBs.pool(host,port,timeout,genericObjectPoolConfig);
    }
}
