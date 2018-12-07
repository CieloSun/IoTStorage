package com.cielo.demo.model.device;

import lombok.Data;

@Data
//典型的路灯上报信息类
public class ElectricityInfo {
    private double i;//当前电流
    private double rate;//当前亮度
    private double v;//当前电压
    private double temperature;//当前温度
    private long runTime;//开灯时长
}
