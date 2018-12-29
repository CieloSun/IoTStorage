package com.cielo.demo.service.scheduled;

import com.cielo.demo.service.config.DeviceConfig;
import com.cielo.demo.service.core.DeviceService;
import com.cielo.storage.api.TimeDataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class DeviceScheduled {
    @Autowired
    private TimeDataUtil timeDataUtil;
    @Autowired
    private DeviceService deviceService;
    @Autowired
    private DeviceConfig deviceConfig;

    //每周三15:00
    @Scheduled(cron = "0 0 15 ? * 3")
    public void archive() {
        deviceConfig.getArchiveFunctionIdList().parallelStream().forEach(functionId -> deviceService.getAllDeviceId().parallelStream().forEach(deviceId -> timeDataUtil.archive(deviceService.deviceTag(deviceId, functionId))));
    }
}
