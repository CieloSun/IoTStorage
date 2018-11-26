package com.cielo.controller;


import com.cielo.model.device.Device;
import com.cielo.model.device.DeviceInfoModel;
import com.cielo.model.user.Permission;
import com.cielo.service.DeviceService;
import com.cielo.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Set;

@RestController
@CrossOrigin
@RequestMapping("device")
public class DeviceController {
    @Autowired
    private DeviceService deviceService;
    @Autowired
    private UserService userService;

    @PostMapping("{token}")
    public void editDevice(@PathVariable String token, @RequestBody Device device) throws Exception {
        userService.authPermission(token, Permission.EDIT_DEVICE);
        deviceService.editDevice(device);
    }

    @GetMapping("{token}")
    public Device getDevice(@PathVariable String token, String deviceId) throws Exception {
        return deviceService.getDevice(deviceId);
    }

    @PostMapping("info/{token}")
    public void save(@PathVariable String token, @RequestBody DeviceInfoModel deviceInfoModel) {
        deviceService.saveDeviceInfo(deviceInfoModel);
    }

    @GetMapping("info/{token}")
    public Set<DeviceInfoModel> getDeviceInfo(@PathVariable String token, String deviceId, Integer function, Long startTime, @RequestParam(required = false) Long endTime) {
        return deviceService.getDeviceInfoByTime(deviceId, function, startTime, endTime);
    }

    @GetMapping("latestInfo/{token}")
    public DeviceInfoModel getLatest(@PathVariable String token, String deviceId, Integer functionId) {
        return deviceService.getLatestDeviceInfo(deviceId, functionId);
    }

    @GetMapping("all/{token}")
    public List<Device> getAllDevice(@PathVariable String token) throws Exception {
        userService.authPermission(token, Permission.GET_ALL_DEVICE);
        return deviceService.getAllDevice();
    }

    @GetMapping("allKey/{token}")
    public List<String> getAllDeviceKeys(@PathVariable String token) throws Exception {
        userService.authPermission(token, Permission.GET_ALL_DEVICE);
        return deviceService.getAllDeviceId();
    }


}
