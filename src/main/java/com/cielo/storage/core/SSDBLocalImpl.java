package com.cielo.storage.core;

import org.nutz.ssdb4j.SSDBs;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service("ssdb-local")
@Order(2)
class SSDBLocalImpl extends SSDBUtilImpl {
    @Override
    public void run(String... args) {
        ssdb = SSDBs.pool(ssdbConfig.getHost2(), ssdbConfig.getPort2(), ssdbConfig.getTimeout(), genericObjectPoolConfig());
    }
}
