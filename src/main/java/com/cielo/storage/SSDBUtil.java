package com.cielo.storage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
//@EnableConfigurationProperties(SSDBConfig.class)
public class SSDBUtil {
    @Autowired
    private SSDBConfig ssdbConfig;
    private SSDB ssdb;

    public void init(SSDB ssdb) {
        this.ssdb = ssdb;
    }

    private Map<String, String> getMap(String pattern) {
        return getMap(pattern, ssdbConfig.getScanNumber());
    }

    private Map<String, String> getMap(String pattern, int scanNumber) {
        return getMap(pattern, pattern + '}', scanNumber);
    }

    private Map<String, String> popMap(String pattern, int scanNumber) {
        return popMap(pattern, pattern + '}', scanNumber);
    }

    private Map<String, String> getMap(String fromPattern, String endPattern, int scanNumber) {
        return ssdb.scan(fromPattern, endPattern, scanNumber).mapString();
    }

    private Map<String, String> popMap(String fromPattern, String endPattern, int scanNumber) {
        Map<String, String> map = ssdb.scan(fromPattern, endPattern, scanNumber).mapString();
        multiDel(map.keySet().toArray());
        return map;
    }



    public Response setVal(String key, Object val) {
        return ssdb.set(key, val);
    }

    public Response set(String key, Object val) {
        return ssdb.set(key, JSON.toJSONString(val));
    }

    public Response setx(String key, Object val, int ttl) {
        return ssdb.setx(key, JSON.toJSONString(val), ttl);
    }

    public Response get(String key) {
        return ssdb.get(key);
    }

    public <T> T get(String key, Class<T> clazz, Feature... features) {
        return JSON.parseObject(get(key).asString(), clazz, features);
    }

    public String getMapValues(String pattern, int scanNumber) {
        return JSONUtil.merge(getMap(pattern, scanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    public String popMapValues(String pattern, int scanNumber) {
        return JSONUtil.merge(popMap(pattern, scanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    public String getMapValues(String pattern) {
        return JSONUtil.merge(getMap(pattern, ssdbConfig.getScanNumber()).values().parallelStream().collect(Collectors.toList()));
    }

    public String popMapValues(String pattern) {
        return JSONUtil.merge(popMap(pattern, ssdbConfig.getScanNumber()).values().parallelStream().collect(Collectors.toList()));
    }

    public String getMapValues(String fromPattern, String endPattern, int scanNumber) {
        return JSONUtil.merge(getMap(fromPattern, endPattern, scanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    public String getMapValues(String fromPattern, String endPattern) {
        return JSONUtil.merge(getMap(fromPattern, endPattern, ssdbConfig.getScanNumber()).values().parallelStream().collect(Collectors.toList()));
    }

    public Set<String> getMapKeys(String pattern) {
        return getMap(pattern).keySet();
    }

    public <T> List<T> getMapValues(String pattern, int scanNumber, Class<T> clazz) {
        return JSON.parseArray(getMapValues(pattern, scanNumber), clazz);
    }

    public <T> List<T> getMapValues(String pattern, Class<T> clazz) {
        return getMapValues(pattern, ssdbConfig.getScanNumber(), clazz);
    }

    public <T> List<T> getMapValues(String fromPattern, String endPattern, int scanNumber, Class<T> clazz) {
        return JSON.parseArray(getMapValues(fromPattern, endPattern, scanNumber), clazz);
    }

    public <T> List<T> getMapValues(String fromPattern, String endPattern, Class<T> clazz) {
        return getMapValues(fromPattern, endPattern, ssdbConfig.getScanNumber(), clazz);
    }

    public int count(String pattern, int scanNumber) {
        return getMap(pattern, scanNumber).size();
    }

    public int count(String pattern) {
        return count(pattern, ssdbConfig.getScanNumber());
    }

    public Response expire(String key, int ttl) {
        return ssdb.expire(key, ttl);
    }

    public Response del(String key) {
        return ssdb.del(key);
    }

    @Async
    public Response multiDel(Object[] keys) {
        return ssdb.multi_del(keys);
    }

    public boolean exists(String key) {
        return ssdb.exists(key).asInt() != 0;
    }

    public Response incr(String key) {
        return incr(key, 1);
    }

    public Response incr(String key, int val) {
        return ssdb.incr(key, val);
    }

    public Response decr(String key) {
        return decr(key, 1);
    }

    public Response decr(String key, int val) {
        return ssdb.decr(key, val);
    }
}
