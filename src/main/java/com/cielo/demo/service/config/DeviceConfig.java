package com.cielo.demo.service.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties("device-config")
public class DeviceConfig {
    private List<Integer> archiveFunctionIdList;
}
