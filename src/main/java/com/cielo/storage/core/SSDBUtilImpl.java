package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.cielo.storage.api.SSDBUtil;
import com.cielo.storage.config.SSDBConfig;
import com.cielo.storage.model.DataTag;
import com.cielo.storage.tool.CollectionUtil;
import com.cielo.storage.tool.JSONUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Map;

abstract class SSDBUtilImpl implements CommandLineRunner, SSDBUtil {
    @Autowired
    protected SSDBConfig ssdbConfig;
    protected SSDB ssdb;

    protected GenericObjectPoolConfig genericObjectPoolConfig() {
        //配置线程池
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(ssdbConfig.getMaxIdle());
        genericObjectPoolConfig.setMinIdle(ssdbConfig.getMinIdle());
        genericObjectPoolConfig.setMaxWaitMillis(ssdbConfig.getMaxWait());
        genericObjectPoolConfig.setNumTestsPerEvictionRun(ssdbConfig.getNumTestsPerEvictionRun());
        genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(ssdbConfig.getSoftMinEvictableIdleTimeMillis());
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(ssdbConfig.getTimeBetweenEvictionRunsMillis());
        genericObjectPoolConfig.setTestOnBorrow(ssdbConfig.isTestOnBorrow());
        genericObjectPoolConfig.setTestOnReturn(ssdbConfig.isTestOnReturn());
        genericObjectPoolConfig.setTestWhileIdle(ssdbConfig.isTestWhileIdle());
        genericObjectPoolConfig.setLifo(ssdbConfig.isLifo());
        return genericObjectPoolConfig;
    }

    @Override
    public abstract void run(String... args);

    //设置一个基本类型值
    @Override
    public Response setVal(Object key, Object val) {
        return ssdb.set(key, val);
    }

    //设置JSON对象
    @Override
    public Response set(Object key, Object val) {
        return ssdb.set(key, JSON.toJSONString(val));
    }

    //设置JSON对象并增加时限
    @Override
    public Response setx(Object key, Object val, int ttl) {
        return ssdb.setx(key, JSON.toJSONString(val), ttl);
    }

    //原始get方法用于获取基本值
    @Override
    public Response get(Object key) {
        return ssdb.get(key);
    }

    //获取一个对象并反序列化
    @Override
    public <T> T get(Object key, Class<T> clazz, Feature... features) {
        Response response = get(key);
        if (response.notFound()) return null;
        return JSON.parseObject(response.asString(), clazz, features);
    }

    //基于首尾字符串获取原始MapString
    @Override
    public Map<String, String> scan(Object fromKey, Object endKey) {
        return ssdb.scan(fromKey, endKey, ssdbConfig.getScanNumber()).mapString();
    }

    //基于前缀获取原始MapString
    @Override
    public Map<String, String> scan(Object prefix) {
        return scan(prefix, prefix + "}");
    }

    //获取前缀的key
    @Override
    public List<String> scanKeys(Object prefix) {
        return scanKeys(prefix, prefix + "}");
    }

    //基于首尾字符串获取key
    @Override
    public List<String> scanKeys(Object fromKey, Object endKey) {
        return ssdb.keys(fromKey, endKey, ssdbConfig.getScanNumber()).listString();
    }

    //基于前缀获取值
    @Override
    public String scanValues(Object prefix) {
        return JSONUtil.toList(CollectionUtil.toList(scan(prefix).values()));
    }

    //基于首尾字符串获取值
    @Override
    public String scanValues(Object fromKey, Object endKey) {
        return JSONUtil.toList(CollectionUtil.toList(scan(fromKey, endKey).values()));
    }

    //基于前缀获取值并反序列化
    @Override
    public <T> List<T> scanValues(Object prefix, Class<T> clazz) {
        return JSON.parseArray(scanValues(prefix), clazz);
    }

    //基于首尾字符串获取值并反序列化
    @Override
    public <T> List<T> scanValues(Object fromKey, Object endKey, Class<T> clazz) {
        return JSON.parseArray(scanValues(fromKey, endKey), clazz);
    }

    //基于首尾字符串弹出Map
    @Override
    public Map<String, String> popScan(Object fromKey, Object endKey) {
        Map<String, String> map = scan(fromKey, endKey);
        multiDel(map.keySet().toArray());
        return map;
    }

    //基于前缀弹出Map
    @Override
    public Map<String, String> popScan(Object prefix) {
        return popScan(prefix, prefix + "}");
    }

    @Override
    public Response lowerBoundKey(Object key) {
        return ssdb.keys(key, "", 1);
    }

    @Override
    public Response lowerBound(Object key) {
        return ssdb.scan(key, "", 1);
    }

    @Override
    public <T> T lowerBound(Object key, Class<T> clazz) {
        Response response = lowerBound(key);
        if (response.notFound()) return null;
        return JSON.parseObject(response.asString(), clazz);
    }

    @Override
    public String lowerBoundVal(Object key) {
        return lowerBound(key).mapString().values().iterator().next();
    }

    //获取计数，数量不超过配置中的scanNumber
    @Override
    public int count(Object prefix) {
        return scan(prefix).size();
    }

    //设置超时时间
    @Override
    public Response expire(Object key, int ttl) {
        return ssdb.expire(key, ttl);
    }

    //删除一个key
    @Override
    public Response del(Object key) {
        return ssdb.del(key);
    }

    //删除多个key，异步
    @Override
    @Async
    public Response multiDel(Object[] keys) {
        return ssdb.multi_del(keys);
    }

    @Override
    public Response multiDel(Object prefix, Object fromKey, Object endKey) {
        return ssdb.multi_del(scanKeys(fromKey, endKey).toArray());
    }

    //基于前缀删除多个key，异步
    @Override
    public Response multiDel(Object prefix) {
        return ssdb.multi_del(scanKeys(prefix).toArray());
    }

    //判断一个key是否存在
    @Override
    public boolean exists(Object key) {
        return ssdb.exists(key).asInt() != 0;
    }

    //某key值自增1
    @Override
    public Response incr(Object key) {
        return incr(key, 1);
    }

    //某key值自增
    @Override
    public Response incr(Object key, int val) {
        return ssdb.incr(key, val);
    }

    //某key值自减1
    @Override
    public Response decr(Object key) {
        return decr(key, 1);
    }

    //某key值自减
    @Override
    public Response decr(Object key, int val) {
        return ssdb.decr(key, val);
    }
    //以下部分为hashMap相关操作

    @Override
    public Response hSetVal(DataTag tag, Object key, Object val) {
        return ssdb.hset(tag.toString(), key, val);
    }

    @Override
    public Response hSet(DataTag tag, Object key, Object val) {
        return ssdb.hset(tag.toString(), key, JSON.toJSONString(val));
    }

    @Override
    public Response hMultiSet(DataTag dataTag, Map map) {
        return ssdb.multi_hset(dataTag.toString(), map);
    }

    @Override
    public Response hGet(DataTag tag, Object key) {
        return ssdb.hget(tag.toString(), key);
    }

    @Override
    public <T> T hGet(DataTag tag, Object key, Class<T> clazz, Feature... features) {
        Response response = hGet(tag, key);
        if (response.notFound()) return null;
        return JSON.parseObject(response.asString(), clazz, features);
    }

    @Override
    public Response hMultiGet(DataTag tag, Object... keys) {
        return ssdb.multi_hget(tag.toString(), keys);
    }

    @Override
    public <T> List<T> hMultiGet(Class<T> clazz, DataTag tag, Object... keys) {
        return JSON.parseArray(JSONUtil.toList(CollectionUtil.toList(ssdb.multi_hget(tag.toString(), keys).mapString().values())), clazz);
    }

    @Override
    public Response hGetAll(DataTag tag) {
        return ssdb.hgetall(tag.toString());
    }

    @Override
    public <T> List<T> hGetAll(DataTag tag, Class<T> clazz) {
        return JSON.parseArray(JSONUtil.toList(CollectionUtil.toList(hGetAll(tag).mapString().values())), clazz);
    }

    @Override
    public List<String> hGetAllKeys(DataTag tag) {
        return CollectionUtil.toList(ssdb.hgetall(tag.toString()).mapString().keySet());
    }

    @Override
    public Response hPopAll(DataTag tag) {
        Response response = hGetAll(tag);
        hClear(tag);
        return response;
    }

    @Override
    public <T> List<T> hPopAll(DataTag tag, Class<T> clazz) {
        return JSON.parseArray(JSONUtil.toList(CollectionUtil.toList(hPopAll(tag).mapString().values())), clazz);
    }

    @Override
    public Integer hSize(DataTag tag) {
        return ssdb.hsize(tag.toString()).asInt();
    }

    @Override
    public boolean hExists(DataTag tag, Object key) {
        return ssdb.hexists(tag.toString(), key).asInt() != 0;
    }

    @Override
    public List<String> hScanName(Object fromName, Object endName) {
        return ssdb.hlist(fromName, endName, ssdbConfig.getScanNumber()).listString();
    }

    @Override
    public List<String> hScanName(Object prefix) {
        return hScanName(prefix, prefix + "}");
    }

    @Override
    public List<String> hScanName(DataTag tag) {
        return hScanName(tag.toString());
    }

    @Override
    public List<String> hScanKeys(DataTag tag, Object fromKey, Object endKey) {
        return ssdb.hkeys(tag.toString(), fromKey, endKey, ssdbConfig.getScanNumber()).listString();
    }

    @Override
    public List<String> hScanKeys(DataTag tag, Object prefix) {
        return hScanKeys(tag, prefix, prefix + "}");
    }

    @Override
    public Map<String, String> hScan(DataTag tag, Object fromKey, Object endKey) {
        return ssdb.hscan(tag.toString(), fromKey, endKey, ssdbConfig.getScanNumber()).mapString();
    }

    @Override
    public Map<String, String> hScan(DataTag tag, Object prefix) {
        return hScan(tag, prefix, prefix + "}");
    }

    @Override
    public String hScanValues(DataTag tag, Object fromKey, Object endKey) {
        return JSONUtil.toList(CollectionUtil.toList(hScan(tag, fromKey, endKey).values()));
    }

    @Override
    public String hScanValues(DataTag tag, Object prefix) {
        return JSONUtil.toList(CollectionUtil.toList(hScan(tag, prefix).values()));
    }

    @Override
    public <T> List<T> hScanValues(DataTag tag, Object fromKey, Object endKey, Class<T> clazz) {
        return JSON.parseArray(hScanValues(tag, fromKey, endKey), clazz);
    }

    @Override
    public <T> List<T> hScanValues(DataTag tag, Object prefix, Class<T> clazz) {
        return JSON.parseArray(hScanValues(tag, prefix), clazz);
    }

    @Override
    public <T> Map<Object, T> hScan(DataTag tag, Object fromKey, Object endKey, Class<T> clazz) {
        return JSONUtil.toMap(hScan(tag, fromKey, endKey), clazz);
    }

    @Override
    public <T> Map<Object, T> hScan(DataTag tag, Object prefix, Class<T> clazz) {
        return JSONUtil.toMap(hScan(tag, prefix), clazz);
    }

    @Override
    public Response hLowerBoundKey(DataTag tag, Object key) {
        return ssdb.hkeys(tag.toString(), key, "", 1);
    }

    @Override
    public Response hLowerBound(DataTag tag, Object key) {
        return ssdb.hscan(tag.toString(), key, "", 1);
    }

    @Override
    public <T> T hLowerBound(DataTag tag, Object key, Class<T> clazz) {
        Response response = hLowerBound(tag, key);
        if (response.notFound()) return null;
        return JSON.parseObject(response.asString(), clazz);
    }

    @Override
    public String hLowerBoundVal(DataTag tag, Object key) {
        return hLowerBound(tag, key).mapString().values().iterator().next();
    }

    @Override
    public Response hDel(DataTag tag, Object key) {
        return ssdb.hdel(tag.toString(), key);
    }

    @Override
    @Async
    public Response hDel(DataTag tag, Object fromKey, Object endKey) {
        return ssdb.multi_hdel(tag.toString(), hScanKeys(tag, fromKey, endKey));
    }

    @Override
    @Async
    public Response hClear(DataTag tag) {
        return ssdb.hclear(tag.toString());
    }
}
