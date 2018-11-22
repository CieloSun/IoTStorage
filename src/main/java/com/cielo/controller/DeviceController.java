package com.cielo.controller;


import com.cielo.fastdfs.FDFS;
import com.cielo.model.DeviceModel;
import com.cielo.service.DeviceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("device")
public class DeviceController {
    @Autowired
    private DeviceService deviceService;

    @PostMapping("{token}")
    public void save(@PathVariable String token, @RequestBody DeviceModel deviceModel) {
        deviceService.saveDeviceInfo(deviceModel);
    }

    @GetMapping("latest/{token}")
    public DeviceModel getLatest(@PathVariable String token, String deviceId, Integer functionId) {
        return deviceService.getLatestDeviceInfo(deviceId, functionId);
    }

    @GetMapping({"token"})
    public List<DeviceModel> getDeviceInfo(@PathVariable String token, String deviceId, Integer function, Long startTime, @RequestParam(required = false) Long endTime) {
        return deviceService.getDeviceInfoByTime(deviceId, function, startTime, endTime);
    }

}
