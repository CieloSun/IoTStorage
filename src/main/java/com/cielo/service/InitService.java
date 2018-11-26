package com.cielo.service;

import com.cielo.model.user.Permission;
import com.cielo.model.user.Role;
import com.cielo.model.user.User;
import com.cielo.storage.SSDBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.HashSet;

@Service
@Order(2)
public class InitService implements CommandLineRunner {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private InitConfig initConfig;
    @Autowired
    private SSDBUtil ssdbUtil;

    @Override
    public void run(String... args) {
        if (initConfig.isInitDatabase()) initDatabase();
    }

    //生成数据库初始数据
    public void initDatabase() {
        //清空数据库
        ssdbUtil.multiDel("");
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
                add(Permission.GET_ALL_DEVICE);
                add(Permission.EDIT_DEVICE);
            }
        }));
        //初始化普通用户角色
        ssdbUtil.set(Role.key(Role.GUEST), new Role(Role.GUEST, new HashSet<>()));
        //初始化管理员用户
        ssdbUtil.set(User.key("admin"), new User("admin", "admin", Role.ADMIN));
        logger.info("The storage database has init");
    }
}
