package com.cielo.netty;

import com.alibaba.fastjson.JSON;
import com.cielo.model.DeviceModel;
import com.cielo.service.DeviceService;
import com.cielo.ssdb.SSDBCommon;
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
        DeviceModel deviceModel = JSON.parseObject(msg, DeviceModel.class);
        String deviceId = deviceModel.getDeviceId();
        deviceManager.set(deviceId, ctx.channel());
        switch (deviceModel.getFunctionId()) {
            case DeviceModel.HEARTBEAT:
                deviceModel.setDate(System.currentTimeMillis());
                deviceManager.sendObject(deviceId, deviceModel);
            case DeviceModel.ELECTRICITY:
                //存储信息包
                deviceService.saveDeviceInfo(deviceModel);
                //TODO 处理信息
                //回复终端
                deviceModel.setContext(null);
                deviceModel.setDate(System.currentTimeMillis());
                deviceManager.sendObject(deviceId, deviceModel);
                break;
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        deviceManager.delete(ctx.channel());
    }
}
