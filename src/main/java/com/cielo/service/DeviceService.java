package com.cielo.service;

import com.cielo.model.DeviceModel;
import com.cielo.ssdb.SSDBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DeviceService {
    @Autowired
    private SSDBUtil ssdbUtil;

    public String pattern(String deviceId, Integer functionId) {
        return "device_" + deviceId + "_" + functionId + "_";
    }

    public String key(DeviceModel deviceModel) {
        return "device_" + deviceModel.getDeviceId() + "_" + deviceModel.getFunctionId() + "_" + deviceModel.getDate();
    }

    public String key(String deviceId, Integer functionId, Long date) {
        return "device_" + deviceId + "_" + functionId + "_" + date;
    }

    public String latest(DeviceModel deviceModel) {
        return "latest_device_" + deviceModel.getDeviceId() + "_" + deviceModel.getFunctionId();
    }

    public String latest(String deviceId, Integer functionId) {
        return "latest_device_" + deviceId + "_" + functionId;
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
        return ssdbUtil.getObjects(key(deviceId, functionId, startDate), key(deviceId, functionId, endDate), DeviceModel.class);
    }

    public DeviceModel getDeviceInfo(String deviceId, Integer functionId, Long date) {
        return ssdbUtil.get(key(deviceId, functionId, date), DeviceModel.class);
    }
}
