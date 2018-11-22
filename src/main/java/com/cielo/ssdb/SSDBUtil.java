package com.cielo.ssdb;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import lombok.Data;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@Data
public class SSDBUtil {
    private SSDB ssdb;
    private int defaultScanNumber;
    private int defaultFunctionNumber;

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

    public Integer getInt(String key) {
        return get(key).asInt();
    }

    public String getString(String key) {
        return get(key).asString();
    }

    public <T> T get(String key, Class<T> clazz, Feature... features) {
        return JSON.parseObject(getString(key), clazz, features);
    }

    private Map<String, String> getObjectMap(String pattern, int scanNumber) {
        return getObjectMap(pattern, pattern + '}', scanNumber);
    }

    private Map<String, String> getObjectMap(String fromPattern, String endPattern, int scanNumber) {
        return ssdb.scan(fromPattern, endPattern, scanNumber).mapString();
    }

    private String mergeJson(List<String> jsonObjects) {
        StringBuilder stringBuilder = new StringBuilder("[");
        IntStream.range(0, jsonObjects.size()).forEach(i -> {
            stringBuilder.append(jsonObjects.get(i));
            if (i < jsonObjects.size() - 1) stringBuilder.append(",");
        });
        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    public String getListString(String pattern, int scanNumber) {
        return mergeJson(getObjectMap(pattern, scanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    public String getListString(String pattern) {
        return mergeJson(getObjectMap(pattern, defaultScanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    public String getListString(String fromPattern, String endPattern, int scanNumber) {
        return mergeJson(getObjectMap(fromPattern, endPattern, scanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    public String getListString(String fromPattern, String endPattern) {
        return mergeJson(getObjectMap(fromPattern, endPattern, defaultScanNumber).values().parallelStream().collect(Collectors.toList()));
    }

    public <T> List<T> getObjects(String pattern, int scanNumber, Class<T> clazz) {
        return JSON.parseArray(getListString(pattern, scanNumber), clazz);
    }

    public <T> List<T> getObjects(String pattern, Class<T> clazz) {
        return getObjects(pattern, defaultScanNumber, clazz);
    }

    public <T> List<T> getObjects(String fromPattern, String endPattern, int scanNumber, Class<T> clazz) {
        return JSON.parseArray(getListString(fromPattern, endPattern, scanNumber), clazz);
    }

    public <T> List<T> getObjects(String fromPattern, String endPattern, Class<T> clazz) {
        return getObjects(fromPattern, endPattern, defaultScanNumber, clazz);
    }

    public int count(String pattern, int scanNumber) {
        return getObjectMap(pattern, scanNumber).size();
    }

    public int count(String pattern) {
        return count(pattern, defaultScanNumber);
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
