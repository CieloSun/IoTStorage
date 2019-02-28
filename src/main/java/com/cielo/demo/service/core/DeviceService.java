package com.cielo.demo.service.core;

import com.cielo.demo.model.device.Device;
import com.cielo.demo.model.device.DeviceInfoModel;
import com.cielo.storage.api.KVStoreUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.model.DataTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DeviceService {
    public static final DataTag DEVICE = new DataTag("configDevice");
    @Autowired
    private KVStoreUtil ssdbSync;
    @Autowired
    private TimeDataUtil timeDataUtil;

    public DataTag deviceTag(DeviceInfoModel deviceInfoModel) {
        return deviceTag(deviceInfoModel.getDeviceId(), deviceInfoModel.getFunctionId());
    }

    public DataTag deviceTag(String deviceId, Integer functionId) {
        return new DataTag("device", functionId.toString(), deviceId);
    }

    public void editDevice(Device device) {
        ssdbSync.hSet(DEVICE, device.getDeviceId(), device);
    }

    public void deleteDevice(String deviceId) {
        ssdbSync.hDel(DEVICE, deviceId);
    }

    public Device getDevice(String deviceId) {
        return ssdbSync.hGet(DEVICE, deviceId, Device.class);
    }

    public List<String> getAllDeviceId() {
        return ssdbSync.hGetAllKeys(DEVICE);
    }

    public List<Device> getAllDevice() {
        return ssdbSync.hGetAll(DEVICE, Device.class);
    }

    public List<Device> getDevices(String... keys) {
        return ssdbSync.hMultiGet(Device.class, DEVICE, keys);
    }

    public void saveDeviceInfo(DeviceInfoModel deviceInfoModel) {
        DataTag hName = deviceTag(deviceInfoModel);
        timeDataUtil.set(hName, deviceInfoModel);
    }

    public DeviceInfoModel getLatestDeviceInfo(String deviceId, Integer functionId) throws Exception {
        return timeDataUtil.getLatest(deviceTag(deviceId, functionId), DeviceInfoModel.class);
    }

    //批量归档中获取多个对象
    public Map<Object, DeviceInfoModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate, Long endDate) {
        return timeDataUtil.get(deviceTag(deviceId, functionId), startDate, endDate, DeviceInfoModel.class);
    }

    //批量归档中获取单个对象
    public DeviceInfoModel getDeviceInfo(String deviceId, Integer functionId, Long timestamp) throws Exception {
        return timeDataUtil.get(deviceTag(deviceId, functionId), timestamp, DeviceInfoModel.class);
    }
}
