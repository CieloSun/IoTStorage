package com.cielo.demo.service.core;

import com.cielo.demo.model.device.Device;
import com.cielo.demo.model.device.DeviceInfoModel;
import com.cielo.storage.api.SSDBUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.model.DataTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class DeviceService {
    public static final DataTag DEVICE = new DataTag("configDevice");
    public static final DataTag LATEST = new DataTag("latestDevice");
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private TimeDataUtil timeDataUtil;

    public DataTag hName(DeviceInfoModel deviceInfoModel) {
        return hName(deviceInfoModel.getDeviceId(), deviceInfoModel.getFunctionId());
    }

    public DataTag hName(String deviceId, Integer functionId) {
        return new DataTag("device", deviceId, functionId.toString());
    }

    public void editDevice(Device device) {
        ssdbUtil.hSet(DEVICE, device.getDeviceId(), device);
    }

    public void deleteDevice(String deviceId) {
        ssdbUtil.hDel(DEVICE, deviceId);
    }

    public Device getDevice(String deviceId) {
        return ssdbUtil.hGet(DEVICE, deviceId, Device.class);
    }

    public List<String> getAllDeviceId() {
        return ssdbUtil.hGetAllKeys(DEVICE);
    }

    public List<Device> getAllDevice() {
        return ssdbUtil.hGetAll(DEVICE, Device.class);
    }

    public List<Device> getDevices(String... keys) {
        return ssdbUtil.hMultiGet(Device.class, DEVICE, keys);
    }

    public void saveDeviceInfo(DeviceInfoModel deviceInfoModel) {
        DataTag hName = hName(deviceInfoModel);
        ssdbUtil.hSet(hName, LATEST, deviceInfoModel);
        timeDataUtil.set(hName, deviceInfoModel);
    }

    public DeviceInfoModel getLatestDeviceInfo(String deviceId, Integer functionId) {
        return ssdbUtil.hGet(hName(deviceId, functionId), LATEST, DeviceInfoModel.class);
    }

    //批量归档中获取多个对象
    public Map<Object, DeviceInfoModel> getDeviceInfoByTime(String deviceId, Integer functionId, Long startDate, Long endDate) {
        return timeDataUtil.get(hName(deviceId, functionId), startDate, endDate, DeviceInfoModel.class);
    }

    //批量归档中获取单个对象
    public DeviceInfoModel getDeviceInfo(String deviceId, Integer functionId, Long timestamp) throws Exception {
        return timeDataUtil.get(hName(deviceId, functionId), timestamp, DeviceInfoModel.class);
    }
}
