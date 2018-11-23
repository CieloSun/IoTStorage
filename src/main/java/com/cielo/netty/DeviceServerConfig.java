package com.cielo.netty;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties("device-server")
public class DeviceServerConfig {
    @Value("42001")
    private int port;
    @Value("300")
    private int readTimeout;
}
