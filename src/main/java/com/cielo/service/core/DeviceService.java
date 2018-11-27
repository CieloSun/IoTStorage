package com.cielo.service.core;

import com.cielo.model.device.Device;
import com.cielo.model.device.DeviceInfoModel;
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
        return "device_" + deviceInfoModel.getFunctionId() + "_" + deviceInfoModel.getDeviceId() + "_" + deviceInfoModel.getDate();
    }

    private String key(String deviceId, Integer functionId, Long date) {
        return "device_" + functionId + "_" + deviceId + "_" + date;
    }

    private String latest(DeviceInfoModel deviceInfoModel) {
        return "latest_device_" + deviceInfoModel.getFunctionId() + "_" + deviceInfoModel.getDeviceId();
    }

    private String latest(String deviceId, Integer functionId) {
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
        if (deviceInfoModel.getDate() == null) {
            deviceInfoModel.setDate(System.currentTimeMillis());
        }
        ssdbUtil.set(key(deviceInfoModel), deviceInfoModel);
        ssdbUtil.set(latest(deviceInfoModel), deviceInfoModel);
    }

    public DeviceInfoModel getLatestDeviceInfo(String deviceId, Integer functionId) {
        return ssdbUtil.get(latest(deviceId, functionId), DeviceInfoModel.class);
    }

    public Set<DeviceInfoModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate) {
        return getDeviceInfoByTime(deviceId, functionId, startDate, System.currentTimeMillis());
    }

    public Set<DeviceInfoModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate, Long endDate) {
        Set<DeviceInfoModel> values = new HashSet<>();
        if (archiveUtil.getLatestArchiveDate(pattern(deviceId, functionId)) < endDate)
            values.addAll(ssdbUtil.getMapValues(key(deviceId, functionId, startDate), key(deviceId, functionId, endDate), DeviceInfoModel.class));
        if (archiveUtil.getLatestArchiveDate(pattern(deviceId, functionId)) > startDate)
            values.addAll(archiveUtil.getObjects(pattern(deviceId, functionId), startDate, endDate, DeviceInfoModel.class)
                    .parallelStream().filter(deviceInfoModel -> deviceInfoModel.getDate() >= startDate && deviceInfoModel.getDate() <= endDate).sorted().collect(Collectors.toList()));
        return values;
    }

    public DeviceInfoModel getDeviceInfo(String deviceId, Integer functionId, Long date) throws Exception {
        if (archiveUtil.getLatestArchiveDate(pattern(deviceId, functionId)) <= date)
            return ssdbUtil.get(key(deviceId, functionId, date), DeviceInfoModel.class);
        else {
            Optional<DeviceInfoModel> optional = archiveUtil.getObjects(pattern(deviceId, functionId), date, DeviceInfoModel.class).parallelStream().filter(deviceInfoModel -> deviceInfoModel.getDate() == date).findAny();
            if (optional.isPresent()) return optional.get();
            else throw new Exception("Cannot find info.");
        }
    }
}
