package com.cielo.storage;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.SSDBs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service
@Order(1)
//@EnableConfigurationProperties(SSDBConfig.class)
public class SSDBLoader implements CommandLineRunner {
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private SSDBConfig ssdbConfig;

    @Override
    public void run(String... args) {
        //配置线程池
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(ssdbConfig.getMaxIdle());
        genericObjectPoolConfig.setMinIdle(ssdbConfig.getMinIdle());
        genericObjectPoolConfig.setMaxWaitMillis(ssdbConfig.getMaxWait());
        genericObjectPoolConfig.setNumTestsPerEvictionRun(ssdbConfig.getNumTestsPerEvictionRun());
        genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(ssdbConfig.getSoftMinEvictableIdleTimeMillis());
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(ssdbConfig.getTimeBetweenEvictionRunsMillis());
        genericObjectPoolConfig.setTestOnBorrow(ssdbConfig.isTestOnBorrow());
        genericObjectPoolConfig.setTestOnReturn(ssdbConfig.isTestOnReturn());
        genericObjectPoolConfig.setTestWhileIdle(ssdbConfig.isTestWhileIdle());
        genericObjectPoolConfig.setLifo(ssdbConfig.isLifo());
        //生成ssdb对象
        ssdbUtil.init(SSDBs.pool(ssdbConfig.getHost(), ssdbConfig.getPort(), ssdbConfig.getTimeout(), genericObjectPoolConfig));
    }
}
