package com.cielo.netty.core;

import com.cielo.netty.config.DeviceServerConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.json.JsonObjectDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;


@Service
@Order(4)
public class DeviceServer implements CommandLineRunner {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private DeviceServerConfig deviceServerConfig;

    private EventLoopGroup bossGroup = new NioEventLoopGroup();
    private EventLoopGroup workerGroup = new NioEventLoopGroup();
    private ServerBootstrap bootstrap = new ServerBootstrap();

    @Override
    @Async
    public void run(String... args) {
        try {
            bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).option(ChannelOption.SO_KEEPALIVE, true).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) {
                    ChannelPipeline pipeline = socketChannel.pipeline();
                    pipeline.addLast(new ReadTimeoutHandler(deviceServerConfig.getReadTimeout()));
                    pipeline.addLast(new JsonObjectDecoder());
                    pipeline.addLast(new StringDecoder());
                    pipeline.addLast(new StringEncoder());
                }
            }).childOption(ChannelOption.SO_KEEPALIVE, true);
            logger.info("Device server setup at port " + deviceServerConfig.getPort());
            ChannelFuture channelFuture = bootstrap.bind(deviceServerConfig.getPort()).sync();
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e) {
            logger.info("Device server setup failed.");
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
