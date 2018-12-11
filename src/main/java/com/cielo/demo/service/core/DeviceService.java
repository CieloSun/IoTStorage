package com.cielo.demo.service.core;

import com.cielo.demo.model.device.Device;
import com.cielo.demo.model.device.DeviceInfoModel;
import com.cielo.storage.core.ArchiveUtil;
import com.cielo.storage.core.SSDBUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DeviceService {
    public static final String DEVICE = "config_device";
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private ArchiveUtil archiveUtil;

    public String pattern(String deviceId, Integer functionId) {
        return "device_" + functionId + "_" + deviceId;
    }

    private String key(DeviceInfoModel deviceInfoModel) {
        return "device_" + deviceInfoModel.getFunctionId() + "_" + deviceInfoModel.getDeviceId() + "_" + deviceInfoModel.getTimestamp();
    }

    //终端key，格式为device_{功能码}_{终端号}_{timestamp}
    private String key(String deviceId, Integer functionId, Long timestamp) {
        return "device_" + functionId + "_" + deviceId + "_" + timestamp;
    }

    //根据终端id，终端功能生成latest键
    private String latestKey(DeviceInfoModel deviceInfoModel) {
        return "latest_device_" + deviceInfoModel.getFunctionId() + "_" + deviceInfoModel.getDeviceId();
    }

    private String latestKey(String deviceId, Integer functionId) {
        return "latest_device_" + functionId + "_" + deviceId;
    }

    public void editDevice(Device device) {
        ssdbUtil.hset(DEVICE, device.getDeviceId(), device);
    }

    public void deleteDevice(String deviceId) {
        ssdbUtil.hdel(DEVICE, deviceId);
    }

    public Device getDevice(String deviceId) {
        return ssdbUtil.hget(DEVICE, deviceId, Device.class);
    }

    public List<String> getAllDeviceId() {
        return ssdbUtil.hgetallKeys(DEVICE);
    }

    public List<Device> getAllDevice() {
        return ssdbUtil.hgetall(DEVICE, Device.class);
    }

    public List<Device> getDevices(String... keys) {
        return ssdbUtil.multiHget(Device.class, DEVICE, keys);
    }

    public void saveDeviceInfo(DeviceInfoModel deviceInfoModel) {
        if (deviceInfoModel.getTimestamp() == null) {
            deviceInfoModel.setTimestamp(System.currentTimeMillis());
        }
        ssdbUtil.set(key(deviceInfoModel), deviceInfoModel);
        ssdbUtil.set(latestKey(deviceInfoModel), deviceInfoModel);
    }

    public DeviceInfoModel getLatestDeviceInfo(String deviceId, Integer functionId) {
        return ssdbUtil.get(latestKey(deviceId, functionId), DeviceInfoModel.class);
    }

    public Set<DeviceInfoModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate) {
        return getDeviceInfoByTime(deviceId, functionId, startDate, System.currentTimeMillis());
    }

    //批量归档中获取多个对象
    public Set<DeviceInfoModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate, Long endDate) {
        Set<DeviceInfoModel> values = new HashSet<>();
        if (archiveUtil.getLatestFileTime(pattern(deviceId, functionId)) < endDate)
            values.addAll(ssdbUtil.getValues(key(deviceId, functionId, startDate), key(deviceId, functionId, endDate), DeviceInfoModel.class));
        if (archiveUtil.getLatestFileTime(pattern(deviceId, functionId)) > startDate)
            values.addAll(archiveUtil.get(pattern(deviceId, functionId), startDate, endDate, DeviceInfoModel.class)
                    .parallelStream().filter(deviceInfoModel -> deviceInfoModel.getTimestamp() >= startDate && deviceInfoModel.getTimestamp() <= endDate).sorted().collect(Collectors.toList()));
        return values;
    }

    //批量归档中获取单个对象
    public DeviceInfoModel getDeviceInfo(String deviceId, Integer functionId, Long timestamp) throws Exception {
        if (archiveUtil.getLatestFileTime(pattern(deviceId, functionId)) <= timestamp)
            return ssdbUtil.get(key(deviceId, functionId, timestamp), DeviceInfoModel.class);
        else {
            Optional<DeviceInfoModel> optional = archiveUtil.get(pattern(deviceId, functionId), timestamp, DeviceInfoModel.class).parallelStream().filter(deviceInfoModel -> deviceInfoModel.getTimestamp() == timestamp).findAny();
            if (optional.isPresent()) return optional.get();
            else throw new Exception("Cannot find info.");
        }
    }
}
