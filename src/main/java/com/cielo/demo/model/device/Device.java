package com.cielo.demo.model.device;

import lombok.Data;

@Data
public class Device {
    private String deviceId;
    private Double longitude;
    private Double latitude;
    private String tag;
    public Device(String deviceId){
        this.deviceId = deviceId;
    }

    public Device(String deviceId, String tag) {
        this.deviceId = deviceId;
        this.tag = tag;
    }

    public Device(String deviceId, Double longitude, Double latitude) {
        this.deviceId = deviceId;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public Device(String deviceId, Double longitude, Double latitude, String tag) {
        this.deviceId = deviceId;
        this.longitude = longitude;
        this.latitude = latitude;
        this.tag = tag;
    }
}
