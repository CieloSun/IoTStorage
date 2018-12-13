package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.cielo.storage.config.SSDBConfig;
import com.cielo.storage.tool.CollectionUtil;
import com.cielo.storage.tool.JSONUtil;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class SSDBUtil {
    @Autowired
    private SSDBConfig ssdbConfig;
    private SSDB ssdb;

    //初始化
    public void init(SSDB ssdb) {
        this.ssdb = ssdb;
    }

    //设置一个基本类型值
    public Response setVal(Object key, Object val) {
        return ssdb.set(key, val);
    }

    //设置JSON对象
    public Response set(Object key, Object val) {
        return ssdb.set(key, JSON.toJSONString(val));
    }

    //设置JSON对象并增加时限
    public Response setx(Object key, Object val, int ttl) {
        return ssdb.setx(key, JSON.toJSONString(val), ttl);
    }

    //原始get方法用于获取基本值
    public Response get(Object key) {
        return ssdb.get(key);
    }

    //获取一个对象并反序列化
    public <T> T get(Object key, Class<T> clazz, Feature... features) {
        Response response = get(key);
        if (response.notFound()) return null;
        return JSON.parseObject(response.asString(), clazz, features);
    }

    //基于首尾字符串获取原始MapString
    public Map<String, String> scan(Object fromKey, Object endKey) {
        return ssdb.scan(fromKey, endKey, ssdbConfig.getScanNumber()).mapString();
    }

    //基于前缀获取原始MapString
    public Map<String, String> scan(Object prefix) {
        return scan(prefix, prefix + "}");
    }

    //获取前缀的key
    public List<String> scanKeys(Object prefix) {
        return scanKeys(prefix, prefix + "}");
    }

    //基于首尾字符串获取key
    public List<String> scanKeys(Object fromKey, Object endKey) {
        return ssdb.keys(fromKey, endKey, ssdbConfig.getScanNumber()).listString();
    }

    //基于前缀获取值
    public String scanValues(Object prefix) {
        return JSONUtil.toList(CollectionUtil.toList(scan(prefix).values()));
    }

    //基于首尾字符串获取值
    public String scanValues(Object fromKey, Object endKey) {
        return JSONUtil.toList(CollectionUtil.toList(scan(fromKey, endKey).values()));
    }

    //基于前缀获取值并反序列化
    public <T> List<T> scanValues(Object prefix, Class<T> clazz) {
        return JSON.parseArray(scanValues(prefix), clazz);
    }

    //基于首尾字符串获取值并反序列化
    public <T> List<T> scanValues(Object fromKey, Object endKey, Class<T> clazz) {
        return JSON.parseArray(scanValues(fromKey, endKey), clazz);
    }

    //基于首尾字符串弹出Map
    public Map<String, String> popScan(Object fromKey, Object endKey) {
        Map<String, String> map = scan(fromKey, endKey);
        multiDel(map.keySet().toArray());
        return map;
    }

    //基于前缀弹出Map
    public Map<String, String> popScan(Object prefix) {
        return popScan(prefix, prefix + "}");
    }

    //获取计数，数量不超过配置中的scanNumber
    public int count(Object prefix) {
        return scan(prefix).size();
    }

    //设置超时时间
    public Response expire(Object key, int ttl) {
        return ssdb.expire(key, ttl);
    }

    //删除一个key
    public Response del(Object key) {
        return ssdb.del(key);
    }

    //基于前缀删除多个key，异步
    public Response multiDel(Object prefix) {
        return ssdb.multi_del(scanKeys(prefix).toArray());
    }

    //删除多个key，异步
    @Async
    public Response multiDel(Object[] keys) {
        return ssdb.multi_del(keys);
    }

    //判断一个key是否存在
    public boolean exists(Object key) {
        return ssdb.exists(key).asInt() != 0;
    }

    //某key值自增1
    public Response incr(Object key) {
        return incr(key, 1);
    }

    //某key值自增
    public Response incr(Object key, int val) {
        return ssdb.incr(key, val);
    }

    //某key值自减1
    public Response decr(Object key) {
        return decr(key, 1);
    }

    //某key值自减
    public Response decr(Object key, int val) {
        return ssdb.decr(key, val);
    }
    //以下部分为hashMap相关操作

    public Response hSetVal(String name, Object key, Object val) {
        return ssdb.hset(name, key, val);
    }

    public Response hSet(String name, Object key, Object val) {
        return ssdb.hset(name, key, JSON.toJSONString(val));
    }

    public Response hGet(String name, Object key) {
        return ssdb.hget(name, key);
    }

    public <T> T hGet(String name, Object key, Class<T> clazz, Feature... features) {
        Response response = hGet(name, key);
        if (response.notFound()) return null;
        return JSON.parseObject(response.asString(), clazz, features);
    }

    public Response multiHGet(String name, Object... keys) {
        return ssdb.multi_hget(name, keys);
    }

    public <T> List<T> multiHGet(Class<T> clazz, String name, Object... keys) {
        return JSON.parseArray(JSONUtil.toList(CollectionUtil.toList(ssdb.multi_hget(name, keys).mapString().values())), clazz);
    }

    public Response hGetAll(String name) {
        return ssdb.hgetall(name);
    }

    public <T> List<T> hGetAll(String name, Class<T> clazz) {
        return JSON.parseArray(JSONUtil.toList(CollectionUtil.toList(hGetAll(name).mapString().values())), clazz);
    }

    public List<String> hGetAllKeys(String name) {
        return CollectionUtil.toList(ssdb.hgetall(name).mapString().keySet());
    }

    public Response hPopAll(String name) {
        Response response = hGetAll(name);
        hClear(name);
        return response;
    }

    public <T> List<T> hPopAll(String name, Class<T> clazz) {
        return JSON.parseArray(JSONUtil.toList(CollectionUtil.toList(hPopAll(name).mapString().values())), clazz);
    }

    public Response hDel(String name, Object key) {
        return ssdb.hdel(name, key);
    }

    public Integer hSize(String name) {
        return ssdb.hsize(name).asInt();
    }

    public boolean hExists(String name, Object key) {
        return ssdb.hexists(name, key).asInt() != 0;
    }

    public List<String> hScanName(Object fromName, Object endName) {
        return ssdb.hlist(fromName, endName, ssdbConfig.getScanNumber()).listString();
    }

    public List<String> hScanName(Object prefix) {
        return hScanName(prefix, prefix + "}");
    }

    public List<String> hScanKeys(String name, Object fromKey, Object endKey) {
        return ssdb.hkeys(name, fromKey, endKey, ssdbConfig.getScanNumber()).listString();
    }

    public List<String> hScanKeys(String name, Object prefix) {
        return hScanKeys(name, prefix, prefix + "}");
    }

    public Map<String, String> hScan(String name, Object fromKey, Object endKey) {
        return ssdb.hscan(name, fromKey, endKey, ssdbConfig.getScanNumber()).mapString();
    }

    public Map<String, String> hScan(String name, Object prefix) {
        return hScan(name, prefix, prefix + "}");
    }

    public String hScanValues(String name, Object fromKey, Object endKey) {
        return JSONUtil.toList(CollectionUtil.toList(hScan(name, fromKey, endKey).values()));
    }

    public String hScanValues(String name, Object prefix) {
        return JSONUtil.toList(CollectionUtil.toList(hScan(name, prefix).values()));
    }

    public <T> List<T> hScanValues(String name, Object fromKey, Object endKey, Class<T> clazz) {
        return JSON.parseArray(hScanValues(name, fromKey, endKey), clazz);
    }

    public <T> List<T> hScanValues(String name, Object prefix, Class<T> clazz) {
        return JSON.parseArray(hScanValues(name, prefix), clazz);
    }

    public <T> Map<Object, T> hScan(String name, Object fromKey, Object endKey, Class<T> clazz) {
        return JSONUtil.toMap(hScan(name, fromKey, endKey), clazz);
    }

    public <T> Map<Object, T> hScan(String name, Object prefix, Class<T> clazz) {
        return JSONUtil.toMap(hScan(name, prefix), clazz);
    }

    @Async
    public Response hClear(String name) {
        return ssdb.hclear(name);
    }
}
