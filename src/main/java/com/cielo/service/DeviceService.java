package com.cielo.service;

import com.cielo.model.DeviceModel;
import com.cielo.storage.ArchiveUtil;
import com.cielo.storage.SSDBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DeviceService {
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private ArchiveUtil archiveUtil;

    public String pattern(String deviceId, Integer functionId) {
        return "device_" + functionId + "_" + deviceId;
    }

    private String key(DeviceModel deviceModel) {
        return "device_" + deviceModel.getFunctionId() + "_" + deviceModel.getDeviceId() + "_" + deviceModel.getDate();
    }

    private String key(String deviceId, Integer functionId, Long date) {
        return "device_" + functionId + "_" + deviceId + "_" + date;
    }

    private String latest(DeviceModel deviceModel) {
        return "latest_device_" + deviceModel.getFunctionId() + "_" + deviceModel.getDeviceId();
    }

    private String latest(String deviceId, Integer functionId) {
        return "latest_device_" + functionId + "_" + deviceId;
    }

    public void saveDeviceInfo(DeviceModel deviceModel) {
        if (deviceModel.getDate() == null) {
            deviceModel.setDate(System.currentTimeMillis());
        }
        ssdbUtil.set(key(deviceModel), deviceModel);
        ssdbUtil.set(latest(deviceModel), deviceModel);
    }

    public DeviceModel getLatestDeviceInfo(String deviceId, Integer functionId) {
        return ssdbUtil.get(latest(deviceId, functionId), DeviceModel.class);
    }

    public Set<DeviceModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate) {
        return getDeviceInfoByTime(deviceId, functionId, startDate, System.currentTimeMillis());
    }

    public Set<DeviceModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate, Long endDate) {
        Set<DeviceModel> values = new HashSet<>();
        if (archiveUtil.getLatestArchiveDate(pattern(deviceId, functionId)) < endDate)
            values.addAll(ssdbUtil.getMapValues(key(deviceId, functionId, startDate), key(deviceId, functionId, endDate), DeviceModel.class));
        if (archiveUtil.getLatestArchiveDate(pattern(deviceId, functionId)) > startDate)
            values.addAll(archiveUtil.getObjects(pattern(deviceId, functionId), startDate, endDate, DeviceModel.class)
                    .parallelStream().filter(deviceModel -> deviceModel.getDate() >= startDate && deviceModel.getDate() <= endDate).sorted().collect(Collectors.toList()));
        return values;
    }

    public DeviceModel getDeviceInfo(String deviceId, Integer functionId, Long date) throws Exception {
        if (archiveUtil.getLatestArchiveDate(pattern(deviceId, functionId)) <= date)
            return ssdbUtil.get(key(deviceId, functionId, date), DeviceModel.class);
        else {
            Optional<DeviceModel> optional = archiveUtil.getObjects(pattern(deviceId, functionId), date, DeviceModel.class).parallelStream().filter(deviceModel -> deviceModel.getDate() == date).findAny();
            if (optional.isPresent()) return optional.get();
            else throw new Exception("Cannot find info.");
        }
    }
}
