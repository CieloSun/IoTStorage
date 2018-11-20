package com.cielo.service;

import com.cielo.model.DeviceModel;
import com.cielo.ssdb.SSDBCommon;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DeviceService {
    @Autowired
    private SSDBCommon ssdbCommon;

    private String key(DeviceModel deviceModel) {
        return "device_" + deviceModel.getDeviceId() + "_" + deviceModel.getFunctionId() + "_" + deviceModel.getDate();
    }

    private String key(String deviceId, Integer functionId, Long date) {
        return "device_" + deviceId + "_" + functionId + "_" + date;
    }

    private String latest(DeviceModel deviceModel) {
        return "latest_device_" + deviceModel.getDeviceId() + "_" + deviceModel.getFunctionId();
    }

    private String latest(String deviceId, Integer functionId) {
        return "latest_device_" + deviceId + "_" + functionId;
    }

    public void saveDeviceInfo(DeviceModel deviceModel) {
        if (deviceModel.getDate() == null) {
            deviceModel.setDate(System.currentTimeMillis());
        }
        ssdbCommon.setObject(key(deviceModel), deviceModel);
        ssdbCommon.setObject(latest(deviceModel), deviceModel);
    }

    public DeviceModel getDeviceInfo(String deviceId, Integer functionId, Long date) {
        return ssdbCommon.getObject(key(deviceId, functionId, date), DeviceModel.class);
    }

    public DeviceModel getLatestDeviceInfo(String deviceId, Integer functionId) {
        return ssdbCommon.getObject(latest(deviceId, functionId), DeviceModel.class);
    }

    public String getLatestDeviceInfoKey(String deviceId, Integer functionId) {
        return key(getLatestDeviceInfo(deviceId, functionId));
    }

    public List<DeviceModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate) {
        return getDeviceInfoByTime(deviceId, functionId, startDate, System.currentTimeMillis());
    }

    public List<DeviceModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate, Long endDate) {
        if (endDate == null) {
            endDate = System.currentTimeMillis();
        }
        return ssdbCommon.getArrayObject(key(deviceId, functionId, startDate), key(deviceId, functionId, endDate), DeviceModel.class);
    }
}
