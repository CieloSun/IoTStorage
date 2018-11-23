package com.cielo.model;

import lombok.Data;
import org.jetbrains.annotations.NotNull;

@Data
public class DeviceModel<T> implements Comparable<DeviceModel<T>> {
    public static final int ELECTRICITY = 0;
    public static final int CONFIG = 1;
    public static final int SETTING = 2;
    public static final int HEARTBEAT = 3;

    private String deviceId;
    private Integer functionId;
    private Long date;
    private Long seriesNumber;
    private T context;

    @Override
    public int compareTo(@NotNull DeviceModel<T> deviceModel) {
        int functionIdCompareResult = functionId.compareTo(deviceModel.getFunctionId());
        int deviceIdCompareResult = deviceId.compareTo(deviceModel.getDeviceId());
        return deviceIdCompareResult == 0 ? functionIdCompareResult == 0 ? date.compareTo(deviceModel.date) : functionIdCompareResult : deviceIdCompareResult;
    }
}
