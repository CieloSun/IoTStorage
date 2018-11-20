package com.cielo.model;

import lombok.Data;

@Data
public class DeviceModel<T> {
    public static final int ELECTRICITY = 0;
    public static final int CONFIG = 1;
    public static final int SETTING = 2;
    public static final int HEARTBEAT = 3;

    private String deviceId;
    private int functionId;
    private Long date;
    private Long seriesNumber;
    private T context;
}
