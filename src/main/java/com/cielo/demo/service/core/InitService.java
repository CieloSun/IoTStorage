package com.cielo.demo.service.core;

import com.cielo.demo.model.device.Device;
import com.cielo.demo.model.device.DeviceInfoModel;
import com.cielo.demo.model.device.ElectricityInfo;
import com.cielo.demo.model.user.Permission;
import com.cielo.demo.model.user.Role;
import com.cielo.demo.model.user.User;
import com.cielo.demo.service.config.InitConfig;
import com.cielo.storage.core.SSDBUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.stream.IntStream;

@Service
@Order(2)
public class InitService implements CommandLineRunner {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private InitConfig initConfig;
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private DeviceService deviceService;

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
        //初始化一个路灯
        deviceService.editDevice(new Device("changhe01"));
        //初始化一些数据
        IntStream.range(0, 10).forEach(i -> {
            DeviceInfoModel<ElectricityInfo> deviceInfoModel = new DeviceInfoModel<>();
            deviceInfoModel.setDeviceId("changhe01");
            deviceInfoModel.setFunctionId(DeviceInfoModel.ELECTRICITY);
            deviceInfoModel.setSeriesNumber(i);
            deviceInfoModel.setTimestamp(System.currentTimeMillis());
            ElectricityInfo electricityInfo = new ElectricityInfo();
            electricityInfo.setI(5);
            electricityInfo.setRate(85);
            electricityInfo.setRunTime(12 * 60 * 60);
            electricityInfo.setTemperature(40);
            electricityInfo.setV(220);
            deviceInfoModel.setContext(electricityInfo);
            deviceService.saveDeviceInfo(deviceInfoModel);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        logger.info("The storage database has init");
    }
}
