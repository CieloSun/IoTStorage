package com.cielo.storage.core;

import org.nutz.ssdb4j.SSDBs;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service
@Primary
@Order(2)
class SSDBSyncImpl extends SSDBUtilImpl {
    @Override
    public void run(String... args) {
        ssdb = SSDBs.pool(ssdbConfig.getHost(), ssdbConfig.getPort(), ssdbConfig.getTimeout(), genericObjectPoolConfig());
    }
}
