package com.cielo.demo.netty.core;

import com.alibaba.fastjson.JSON;
import com.cielo.demo.model.device.DeviceInfoModel;
import com.cielo.demo.service.core.DeviceService;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

@Service
@Scope("prototype")
public class DeviceServerHandler extends SimpleChannelInboundHandler<String> {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private DeviceManager deviceManager;
    @Autowired
    private DeviceService deviceService;

    @Override
    public void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        DeviceInfoModel deviceInfoModel = JSON.parseObject(msg, DeviceInfoModel.class);
        String deviceId = deviceInfoModel.getDeviceId();
        deviceManager.set(deviceId, ctx.channel());
        switch (deviceInfoModel.getFunctionId()) {
            case DeviceInfoModel.HEARTBEAT:
                deviceInfoModel.setTimestamp(System.currentTimeMillis());
                deviceManager.sendObject(deviceId, deviceInfoModel);
            case DeviceInfoModel.ELECTRICITY:
                //存储信息包
                deviceService.saveDeviceInfo(deviceInfoModel);
                //TODO 处理信息
                //回复终端
                deviceInfoModel.setContext(null);
                deviceInfoModel.setTimestamp(System.currentTimeMillis());
                deviceManager.sendObject(deviceId, deviceInfoModel);
                break;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        deviceManager.delete(ctx.channel());
    }
}
