package com.cielo.netty;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class DeviceManager {
    private volatile Map<String, Channel> channels = new ConcurrentHashMap<>();

    public void set(String id, Channel channel) {
        channels.put(id, channel);
    }

    public Channel get(String id) {
        return channels.get(id);
    }

    public boolean exists(String id) {
        return channels.containsKey(id);
    }

    public void delete(String id) {
        channels.remove(id);
    }

    public void deleteAll() {
        channels.clear();
    }

    public void delete(Channel channel) {
        channels.keySet().parallelStream().filter(id -> channels.get(id) == channel).forEach(id -> channels.remove(id));
    }

    public boolean sendVal(String id, Object message) {
        final Channel channel = channels.get(id);
        if (channel == null) return false;
        channel.writeAndFlush(message);
        return true;
    }

    public boolean sendObject(String id, Object object) {
        return sendVal(id, JSON.toJSONString(object));
    }
}
