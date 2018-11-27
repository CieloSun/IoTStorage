package com.cielo.model.device;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class DeviceInfoModel<T> implements Comparable<DeviceInfoModel<T>> {
    public static final int SETTING = 0;
    public static final int HEARTBEAT = 1;
    public static final int ELECTRICITY = 2;

    private String deviceId;
    private Integer functionId;
    private Long date;
    private Integer seriesNumber;
    private T context;

    @Override
    public int compareTo(@NotNull DeviceInfoModel<T> deviceInfoModel) {
        int functionIdCompareResult = functionId.compareTo(deviceInfoModel.getFunctionId());
        int deviceIdCompareResult = deviceId.compareTo(deviceInfoModel.getDeviceId());
        return deviceIdCompareResult == 0 ? functionIdCompareResult == 0 ? date.compareTo(deviceInfoModel.date) : functionIdCompareResult : deviceIdCompareResult;
    }
}
