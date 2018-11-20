package com.cielo.model;

import lombok.Data;

@Data
//典型的需与路灯交互的配置类
public class DeviceSetting {
    private double maxI;
    private double minI;
    private double maxV;
    private double minV;
    private double maxRate;
    private double minRate;
    private double maxTemperature;
    private Long maxRuntime;
}
