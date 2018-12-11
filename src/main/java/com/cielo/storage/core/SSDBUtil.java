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

    public Response get(String key) {
        return ssdb.get(key);
    }

    public <T> T get(String key, Class<T> clazz, Feature... features) {
        return JSON.parseObject(get(key).asString(), clazz, features);
    }

    public Map<String, String> getMap(String fromPattern, String endPattern) {
        return ssdb.scan(fromPattern, endPattern, ssdbConfig.getScanNumber()).mapString();
    }

    public Map<String, String> getMap(String pattern) {
        return getMap(pattern, pattern + '}');
    }

    public Set<String> getKeys(String pattern) {
        return getMap(pattern).keySet();
    }

    public Set<String> getKeys(String fromPattern, String endPattern) {
        return getMap(fromPattern, endPattern).keySet();
    }

    public String getValues(String pattern) {
        return JSONUtil.merge(getMap(pattern).values().parallelStream().collect(Collectors.toList()));
    }

    public String getValues(String fromPattern, String endPattern) {
        return JSONUtil.merge(getMap(fromPattern, endPattern).values().parallelStream().collect(Collectors.toList()));
    }

    public <T> List<T> getValues(String pattern, Class<T> clazz) {
        return JSON.parseArray(getValues(pattern), clazz);
    }

    public <T> List<T> getValues(String fromPattern, String endPattern, Class<T> clazz) {
        return JSON.parseArray(getValues(fromPattern, endPattern), clazz);
    }

    public Map<String, String> popMap(String fromPattern, String endPattern) {
        Map<String, String> map = ssdb.scan(fromPattern, endPattern, ssdbConfig.getScanNumber()).mapString();
        multiDel(map.keySet().toArray());
        return map;
    }

    public Map<String, String> popMap(String pattern) {
        return popMap(pattern, pattern + '}');
    }

    public int count(String pattern) {
        return getMap(pattern).size();
    }

    public Response expire(String key, int ttl) {
        return ssdb.expire(key, ttl);
    }

    public Response del(String key) {
        return ssdb.del(key);
    }

    public Response multiDel(String pattern) {
        return ssdb.multi_del(getKeys(pattern).toArray());
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


    public Response hsetVal(String name, String key, Object val) {
        return ssdb.hset(name, key, val);
    }

    public Response hset(String name, String key, Object val) {
        return ssdb.hset(name, key, JSON.toJSONString(val));
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
}
