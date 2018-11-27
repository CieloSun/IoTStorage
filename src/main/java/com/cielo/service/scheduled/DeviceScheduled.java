package com.cielo.service.scheduled;

import com.cielo.service.core.DeviceService;
import com.cielo.service.config.DeviceConfig;
import com.cielo.storage.core.ArchiveUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class DeviceScheduled {
    @Autowired
    private ArchiveUtil archiveUtil;
    @Autowired
    private DeviceService deviceService;
    @Autowired
    private DeviceConfig deviceConfig;


    public void archive() {
        deviceConfig.getArchiveFunctionIdList().parallelStream().forEach(functionId -> deviceService.getAllDeviceId().parallelStream().forEach(deviceId -> archiveUtil.archive(deviceService.pattern(deviceId, functionId))));
    }

    //每周三15:00
    @Scheduled(cron = "0 0 15 ? * 3")
    public void scheduled(){
        archive();
    }
}
