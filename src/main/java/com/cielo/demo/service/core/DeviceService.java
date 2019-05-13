package com.cielo.demo.service.core;

import com.cielo.demo.model.device.Device;
import com.cielo.demo.model.device.DeviceInfoModel;
import com.cielo.storage.api.KVStoreUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.model.InternalKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DeviceService {
    public static final InternalKey DEVICE = new InternalKey("configDevice");
    @Autowired
    private KVStoreUtil kvStoreUtil;
    @Autowired
    private TimeDataUtil timeDataUtil;

    public String[] deviceTag(String deviceId, Integer functionId) {
        return new String[]{"function/" + functionId.toString(), "device/" + deviceId};
    }

    public void editDevice(Device device) {
        kvStoreUtil.hSet(DEVICE, device.getDeviceId(), device);
    }

    public void deleteDevice(String deviceId) {
        kvStoreUtil.hDel(DEVICE, deviceId);
    }

    public Device getDevice(String deviceId) {
        return kvStoreUtil.hGet(DEVICE, deviceId, Device.class);
    }

    public List<String> getAllDeviceId() {
        return kvStoreUtil.hGetAllKeys(DEVICE);
    }

    public List<Device> getAllDevice() {
        return kvStoreUtil.hGetAll(DEVICE, Device.class);
    }

    public List<Device> getDevices(String... keys) {
        return kvStoreUtil.hMultiGet(Device.class, DEVICE, keys);
    }

    public void saveDeviceInfo(DeviceInfoModel deviceInfoModel) {
        timeDataUtil.set(deviceTag(deviceInfoModel.getDeviceId(), deviceInfoModel.getFunctionId()), deviceInfoModel);
    }

    public DeviceInfoModel getLatestDeviceInfo(String deviceId, Integer functionId) throws Exception {
        return timeDataUtil.getLatest(deviceTag(deviceId, functionId), DeviceInfoModel.class);
    }

    //批量归档中获取多个对象
    public Map<Object, DeviceInfoModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate, Long endDate) throws Exception {
        return timeDataUtil.get(deviceTag(deviceId, functionId), startDate, endDate, DeviceInfoModel.class);
    }

    //批量归档中获取单个对象
    public DeviceInfoModel getDeviceInfo(String deviceId, Integer functionId, Long timestamp) throws Exception {
        return timeDataUtil.get(deviceTag(deviceId, functionId), timestamp, DeviceInfoModel.class);
    }
}
