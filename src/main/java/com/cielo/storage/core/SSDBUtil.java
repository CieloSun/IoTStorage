package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.cielo.storage.config.SSDBConfig;
import com.cielo.storage.tool.JSONUtil;
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
public class SSDBUtil {
    @Autowired
    private SSDBConfig ssdbConfig;
    private SSDB ssdb;

    private Map<String, String> getMap(String pattern) {
        return getMap(pattern, ssdbConfig.getScanNumber());
    }

    private Map<String, String> getMap(String fromPattern, String endPattern, int scanNumber) {
        return ssdb.scan(fromPattern, endPattern, scanNumber).mapString();
    }

    private Map<String, String> getMap(String pattern, int scanNumber) {
        return getMap(pattern, pattern + '}', scanNumber);
    }

    private String getMapValues(String pattern, int scanNumber) {
        return JSONUtil.merge(getMap(pattern, scanNumber).values().parallelStream().collect(Collectors.toList()));
    }


    private String getMapValues(String fromPattern, String endPattern, int scanNumber) {
        return JSONUtil.merge(getMap(fromPattern, endPattern, scanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    private <T> List<T> getMapValues(String pattern, int scanNumber, Class<T> clazz) {
        return JSON.parseArray(getMapValues(pattern, scanNumber), clazz);
    }

    private <T> List<T> getMapValues(String fromPattern, String endPattern, int scanNumber, Class<T> clazz) {
        return JSON.parseArray(getMapValues(fromPattern, endPattern, scanNumber), clazz);
    }

    private Map<String, String> popMap(String fromPattern, String endPattern, int scanNumber) {
        Map<String, String> map = ssdb.scan(fromPattern, endPattern, scanNumber).mapString();
        multiDel(map.keySet().toArray());
        return map;
    }

    private Map<String, String> popMap(String pattern, int scanNumber) {
        return popMap(pattern, pattern + '}', scanNumber);
    }

    private int count(String pattern, int scanNumber) {
        return getMap(pattern, scanNumber).size();
    }

    public void init(SSDB ssdb) {
        this.ssdb = ssdb;
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

    public Response hsetVal(String name, String key, Object val) {
        return ssdb.hset(name, key, val);
    }

    public Response hset(String name, String key, Object val) {
        return ssdb.hset(name, key, JSON.toJSONString(val));
    }

    public Response get(String key) {
        return ssdb.get(key);
    }

    public Response hget(String name, String key) {
        return ssdb.hget(name, key);
    }

    public Response multiHget(String name, String... keys) {
        return ssdb.multi_hget(name, keys);
    }

    public <T> List<T> multiHget(Class<T> clazz, String name, String... keys) {
        return JSON.parseArray(JSONUtil.merge(ssdb.multi_hget(name, keys).mapString().values().parallelStream().collect(Collectors.toList())), clazz);
    }

    public <T> T hget(String name, String key, Class<T> clazz, Feature... features) {
        return JSON.parseObject(hget(name, key).asString(), clazz, features);
    }

    public Response hgetall(String name) {
        return ssdb.hgetall(name);
    }

    public List<String> hgetallKeys(String name) {
        return ssdb.hgetall(name).mapString().keySet().parallelStream().collect(Collectors.toList());
    }

    public <T> List<T> hgetall(String name, Class<T> clazz) {
        return JSON.parseArray(JSONUtil.merge(ssdb.hgetall(name).mapString().values().parallelStream().collect(Collectors.toList())), clazz);
    }

    public Response hdel(String name, String key) {
        return ssdb.hdel(name, key);
    }

    public Integer hsize(String name) {
        return ssdb.hsize(name).asInt();
    }

    public boolean hexists(String name, String key) {
        return ssdb.hexists(name, key).asInt() != 0;
    }


    public <T> T get(String key, Class<T> clazz, Feature... features) {
        return JSON.parseObject(get(key).asString(), clazz, features);
    }


    public String getMapValues(String pattern) {
        return JSONUtil.merge(getMap(pattern, ssdbConfig.getScanNumber()).values().parallelStream().collect(Collectors.toList()));
    }


    public String getMapValues(String fromPattern, String endPattern) {
        return JSONUtil.merge(getMap(fromPattern, endPattern, ssdbConfig.getScanNumber()).values().parallelStream().collect(Collectors.toList()));
    }

    public Set<String> getMapKeys(String pattern) {
        return getMap(pattern).keySet();
    }

    public <T> List<T> getMapValues(String pattern, Class<T> clazz) {
        return getMapValues(pattern, ssdbConfig.getScanNumber(), clazz);
    }


    public <T> List<T> getMapValues(String fromPattern, String endPattern, Class<T> clazz) {
        return getMapValues(fromPattern, endPattern, ssdbConfig.getScanNumber(), clazz);
    }


    public Map<String, String> popMap(String pattern) {
        return popMap(pattern, ssdbConfig.getScanNumber());
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

    public Response multiDel(String pattern) {
        return ssdb.multi_del(getMapKeys(pattern).toArray());
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
