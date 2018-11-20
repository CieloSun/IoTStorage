package com.cielo.ssdb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class SSDBBase {
    protected SSDB ssdb;

    public Response setVal(String key, Object val) {
        return ssdb.set(key, val);
    }

    public Response setObject(String key, Object val) {
        return ssdb.set(key, JSON.toJSONString(val));
    }

    public Response setObjectWithTTL(String key, Object val, int ttl) {
        return ssdb.setx(key, JSON.toJSONString(val), ttl);
    }

    public Response get(String key) {
        return ssdb.get(key);
    }

    public Integer getInt(String key) {
        return get(key).asInt();
    }

    public String getString(String key) {
        return get(key).asString();
    }

    public <T> T getObject(String key, Class<T> clazz, Feature... features) {
        return JSON.parseObject(getString(key), clazz, features);
    }

    public Map<String, String> getArrayString(String pattern, int scanNumber) {
        return getArrayString(pattern, pattern + '}', scanNumber);
    }

    public Map<String, String> getArrayString(String fromPattern, String endPattern, int scanNumber) {
        return ssdb.scan(fromPattern, endPattern, scanNumber).mapString();
    }

    public <T> List<T> getArrayObject(String pattern, int scanNumber, Class<T> clazz, Feature... features) {
        return getArrayString(pattern, scanNumber).values().parallelStream().map(jsonStr -> JSON.parseObject(jsonStr, clazz, features)).collect(Collectors.toList());
    }

    public <T> List<T> getArrayObject(String fromPattern, String endPattern, int scanNumber, Class<T> clazz, Feature... features) {
        return getArrayString(fromPattern, endPattern, scanNumber).values().parallelStream().map(jsonStr -> JSON.parseObject(jsonStr, clazz, features)).collect(Collectors.toList());
    }

    public int count(String pattern, int scanNumber) {
        return getArrayString(pattern, scanNumber).size();
    }

    public Response expire(String key, int ttl) {
        return ssdb.expire(key, ttl);
    }

    public Response del(String key) {
        return ssdb.del(key);
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
        return ssdb.decr(key, 1);
    }
}
