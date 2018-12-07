package com.cielo.demo.model.device;

import lombok.Data;

@Data
//典型的需与路灯交互的配置类,需人工配置，配置后很少修改
public class SettingInfo {
    private double maxI;
    private double minI;
    private double maxV;
    private double minV;
    private double maxRate;
    private double minRate;
    private double maxTemperature;
    private Long maxRuntime;
}
