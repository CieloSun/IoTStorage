package com.cielo.ssdb;

import com.cielo.model.Permission;
import com.cielo.model.Role;
import com.cielo.model.User;
import lombok.Data;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.SSDBs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashSet;

@Service
@Order(2)
@Data
@Configuration
@ConfigurationProperties("ssdb-common")
public class SSDBLoader implements CommandLineRunner {
    Logger logger = LoggerFactory.getLogger(this.getClass());
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
    @Value("false")
    private boolean init;
    @Value("999")
    private int scanNumber;

    @Autowired
    private SSDBUtil ssdbUtil;


    @Override
    @Async
    public void run(String... args) {
        //配置线程池
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
        //生成ssdb对象
        ssdbUtil.setSsdb(SSDBs.pool(host, port, timeout, genericObjectPoolConfig));
        ssdbUtil.setDefaultScanNumber(scanNumber);
        if (init) initDatabase();
    }

    //生成数据库初始数据
    public void initDatabase() {
        //初始化权限
        ssdbUtil.set(Permission.key(Permission.EDIT_ROLE), new Permission(Permission.EDIT_ROLE, "edit permission"));
        ssdbUtil.set(Permission.key(Permission.EDIT_USER), new Permission(Permission.EDIT_USER, "edit user"));
        ssdbUtil.set(Permission.key(Permission.GET_USER), new Permission(Permission.GET_USER, "get user"));
        ssdbUtil.set(Permission.key(Permission.SHOW_PERMISSION), new Permission(Permission.SHOW_PERMISSION, "show permission"));
        //初始化管理员角色
        ssdbUtil.set(Role.key(Role.ADMIN), new Role(Role.ADMIN, new HashSet<Integer>() {
            {
                add(Permission.EDIT_ROLE);
                add(Permission.GET_USER);
                add(Permission.EDIT_USER);
                add(Permission.SHOW_PERMISSION);
            }
        }));
        //初始化普通用户角色
        ssdbUtil.set(Role.key(Role.GUEST), new Role(Role.GUEST, new HashSet<>()));
        //初始化管理员用户
        ssdbUtil.set(User.key("admin"), new User("admin", "admin", Role.ADMIN));
        logger.info("The ssdb database has init");
    }
}
