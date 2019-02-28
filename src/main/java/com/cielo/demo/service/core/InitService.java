package com.cielo.demo.service.core;

import com.alibaba.fastjson.JSON;
import com.cielo.demo.model.device.Device;
import com.cielo.demo.model.device.DeviceInfoModel;
import com.cielo.demo.model.device.ElectricityInfo;
import com.cielo.demo.model.user.Permission;
import com.cielo.demo.model.user.Role;
import com.cielo.demo.model.user.User;
import com.cielo.demo.service.config.InitConfig;
import com.cielo.storage.api.KVStoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.stream.IntStream;

@Service
@Order(3)
public class InitService implements CommandLineRunner {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private InitConfig initConfig;
    @Autowired
    private KVStoreUtil kvStoreUtil;
    @Autowired
    private DeviceService deviceService;

    @Override
    public void run(String... args) {
        if (initConfig.isInitDatabase()) initDatabase();
    }

    public void initDeviceInfoData(int amount, boolean... flags) {
        //初始化一个路灯
        deviceService.editDevice(new Device("changhe01"));
        if (flags.length > 0 && flags[0]) {
            File file = new File("trainDir");
            if (!file.exists()) file.mkdir();
        }
        //初始化一些数据
        IntStream.range(0, amount).forEach(i -> {
            DeviceInfoModel<ElectricityInfo> deviceInfoModel = new DeviceInfoModel<>();
            deviceInfoModel.setDeviceId("changhe01");
            deviceInfoModel.setFunctionId(DeviceInfoModel.ELECTRICITY);
            deviceInfoModel.setSeriesNumber(i);
            long timestamp = System.currentTimeMillis();
            deviceInfoModel.setTimestamp(timestamp);
            ElectricityInfo electricityInfo = new ElectricityInfo();
            electricityInfo.setI(Math.random() * 10);
            electricityInfo.setRate(80 + Math.random() * 20);
            electricityInfo.setRunTime(12 * 60 * 60);
            electricityInfo.setTemperature(25 + Math.random() * 15);
            electricityInfo.setV(215 + Math.random() * 10);
            deviceInfoModel.setContext(electricityInfo);
            deviceService.saveDeviceInfo(deviceInfoModel);
            if (flags.length > 0 && flags[0]) {
                File jsonFile = new File("trainDir/deviceInfo_" + timestamp + ".json");
                try {
                    FileOutputStream fileOutputStream = new FileOutputStream(jsonFile);
                    fileOutputStream.write(JSON.toJSONBytes(deviceInfoModel));
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
            try {
                Thread.sleep((int) (50 + Math.random() * 100));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

    //生成数据库初始数据
    public void initDatabase() {
        //清空数据库
        kvStoreUtil.multiDel("");
        //初始化权限
        kvStoreUtil.set(Permission.key(Permission.EDIT_ROLE), new Permission(Permission.EDIT_ROLE, "edit permission"));
        kvStoreUtil.set(Permission.key(Permission.EDIT_USER), new Permission(Permission.EDIT_USER, "edit user"));
        kvStoreUtil.set(Permission.key(Permission.GET_USER), new Permission(Permission.GET_USER, "get user"));
        kvStoreUtil.set(Permission.key(Permission.SHOW_PERMISSION), new Permission(Permission.SHOW_PERMISSION, "show permission"));
        //初始化管理员角色
        kvStoreUtil.set(Role.key(Role.ADMIN), new Role(Role.ADMIN, new HashSet<Integer>() {
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
        kvStoreUtil.set(Role.key(Role.GUEST), new Role(Role.GUEST, new HashSet<>()));
        //初始化管理员用户
        kvStoreUtil.set(User.key("admin"), new User("admin", "admin", Role.ADMIN));
        initDeviceInfoData(10);
        //生成海量训练数据
//        initDeviceInfoData(50000, true);
        logger.info("The storage database has init");
    }
}
