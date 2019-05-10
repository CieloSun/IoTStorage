package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.parser.Feature;
import com.cielo.storage.api.KVStoreUtil;
import com.cielo.storage.config.KVStoreConfig;
import com.cielo.storage.model.InternalKey;
import com.cielo.storage.tool.CollectionUtil;
import com.cielo.storage.tool.JSONUtil;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@Primary
@Order(2)
class KVStoreUtilImpl implements CommandLineRunner, KVStoreUtil {
    @Autowired
    private KVStoreConfig KVStoreConfig;
    private SSDB ssdb;

    protected GenericObjectPoolConfig genericObjectPoolConfig() {
        //配置线程池
        GenericObjectPoolConfig genericObjectPoolConfig = new GenericObjectPoolConfig();
        genericObjectPoolConfig.setMaxIdle(KVStoreConfig.getMaxIdle());
        genericObjectPoolConfig.setMinIdle(KVStoreConfig.getMinIdle());
        genericObjectPoolConfig.setMaxWaitMillis(KVStoreConfig.getMaxWait());
        genericObjectPoolConfig.setNumTestsPerEvictionRun(KVStoreConfig.getNumTestsPerEvictionRun());
        genericObjectPoolConfig.setSoftMinEvictableIdleTimeMillis(KVStoreConfig.getSoftMinEvictableIdleTimeMillis());
        genericObjectPoolConfig.setTimeBetweenEvictionRunsMillis(KVStoreConfig.getTimeBetweenEvictionRunsMillis());
        genericObjectPoolConfig.setTestOnBorrow(KVStoreConfig.isTestOnBorrow());
        genericObjectPoolConfig.setTestOnReturn(KVStoreConfig.isTestOnReturn());
        genericObjectPoolConfig.setTestWhileIdle(KVStoreConfig.isTestWhileIdle());
        genericObjectPoolConfig.setLifo(KVStoreConfig.isLifo());
        return genericObjectPoolConfig;
    }

    @Override
    public void run(String... args) {
        ssdb = SSDBs.pool(KVStoreConfig.getHost(), KVStoreConfig.getPort(), KVStoreConfig.getTimeout(), genericObjectPoolConfig());
    }

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
        return ssdb.scan(fromKey, endKey, KVStoreConfig.getScanNumber()).mapString();
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
        return ssdb.keys(fromKey, endKey, KVStoreConfig.getScanNumber()).listString();
    }

    //基于前缀获取值
    @Override
    public String scanValues(Object prefix) {
        return JSONUtil.toListJSON(CollectionUtil.toList(scan(prefix).values()));
    }

    //基于首尾字符串获取值
    @Override
    public String scanValues(Object fromKey, Object endKey) {
        return JSONUtil.toListJSON(CollectionUtil.toList(scan(fromKey, endKey).values()));
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

    @Override
    public Response lowerBoundKey(Object key) {
        return ssdb.keys(key, "", 1);
    }

    @Override
    public Response lowerBound(Object key) {
        return ssdb.scan(key, "", 1);
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
    public Response hSetVal(InternalKey internalKey, Object key, Object val) {
        return ssdb.hset(internalKey.toString(), key, val);
    }

    @Override
    public Response hSet(InternalKey internalKey, Object key, Object val) {
        return ssdb.hset(internalKey.toString(), key, JSON.toJSONString(val));
    }

    @Override
    public Response hMultiSet(InternalKey internalKey, Map map) {
        return ssdb.multi_hset(internalKey.toString(), map);
    }

    @Override
    public Response hGet(InternalKey internalKey, Object key) {
        return ssdb.hget(internalKey.toString(), key);
    }

    @Override
    public <T> T hGet(InternalKey internalKey, Object key, Class<T> clazz, Feature... features) {
        Response response = hGet(internalKey, key);
        if (response.notFound()) return null;
        return JSON.parseObject(response.asString(), clazz, features);
    }

    @Override
    public Response hMultiGet(InternalKey internalKey, Object... keys) {
        return ssdb.multi_hget(internalKey.toString(), keys);
    }

    @Override
    public <T> List<T> hMultiGet(Class<T> clazz, InternalKey internalKey, Object... keys) {
        return JSON.parseArray(JSONUtil.toListJSON(CollectionUtil.toList(ssdb.multi_hget(internalKey.toString(), keys).mapString().values())), clazz);
    }

    @Override
    public Response hGetAll(InternalKey internalKey) {
        return ssdb.hgetall(internalKey.toString());
    }

    @Override
    public <T> List<T> hGetAll(InternalKey internalKey, Class<T> clazz) {
        return JSON.parseArray(JSONUtil.toListJSON(CollectionUtil.toList(hGetAll(internalKey).mapString().values())), clazz);
    }

    @Override
    public List<String> hGetAllKeys(InternalKey internalKey) {
        return CollectionUtil.toList(ssdb.hgetall(internalKey.toString()).mapString().keySet());
    }

    @Override
    public Integer hSize(InternalKey internalKey) {
        return ssdb.hsize(internalKey.toString()).asInt();
    }

    @Override
    public boolean hExists(InternalKey internalKey, Object key) {
        return ssdb.hexists(internalKey.toString(), key).asInt() != 0;
    }

    @Override
    public List<String> hScanName(Object fromName, Object endName) {
        return ssdb.hlist(fromName, endName, KVStoreConfig.getScanNumber()).listString();
    }

    @Override
    public List<String> hScanName(Object prefix) {
        return hScanName(prefix, prefix + "}");
    }

    @Override
    public List<String> hScanName(InternalKey internalKey) {
        return hScanName(internalKey.toString());
    }

    @Override
    public List<String> hScanKeys(InternalKey internalKey, Object fromKey, Object endKey) {
        return ssdb.hkeys(internalKey.toString(), fromKey, endKey, KVStoreConfig.getScanNumber()).listString();
    }

    @Override
    public List<String> hScanKeys(InternalKey internalKey, Object prefix) {
        return hScanKeys(internalKey, prefix, prefix + "}");
    }

    @Override
    public Map<String, String> hScan(InternalKey internalKey, Object fromKey, Object endKey) {
        return ssdb.hscan(internalKey.toString(), fromKey, endKey, KVStoreConfig.getScanNumber()).mapString();
    }

    @Override
    public Map<String, String> hScan(InternalKey internalKey, Object prefix) {
        return hScan(internalKey, prefix, prefix + "}");
    }

    @Override
    public <T> Map<Object, T> hScan(InternalKey internalKey, Object fromKey, Object endKey, Class<T> clazz) {
        return JSONUtil.toMap(hScan(internalKey, fromKey, endKey));
    }

    @Override
    public <T> Map<Object, T> hScan(InternalKey internalKey, Object prefix, Class<T> clazz) {
        return JSONUtil.toMap(hScan(internalKey, prefix));
    }

    @Override
    public String hScanValues(InternalKey internalKey, Object fromKey, Object endKey) {
        return JSONUtil.toListJSON(CollectionUtil.toList(hScan(internalKey, fromKey, endKey).values()));
    }

    @Override
    public String hScanValues(InternalKey internalKey, Object prefix) {
        return JSONUtil.toListJSON(CollectionUtil.toList(hScan(internalKey, prefix).values()));
    }

    @Override
    public <T> List<T> hScanValues(InternalKey internalKey, Object fromKey, Object endKey, Class<T> clazz) {
        return JSON.parseArray(hScanValues(internalKey, fromKey, endKey), clazz);
    }

    @Override
    public <T> List<T> hScanValues(InternalKey internalKey, Object prefix, Class<T> clazz) {
        return JSON.parseArray(hScanValues(internalKey, prefix), clazz);
    }

    @Override
    public Response hLowerBoundKey(InternalKey internalKey, Object key) {
        return ssdb.hkeys(internalKey.toString(), key, "", 1);
    }

    @Override
    public Response hLowerBound(InternalKey internalKey, Object key) {
        return ssdb.hscan(internalKey.toString(), key, "", 1);
    }

    @Override
    public String hLowerBoundVal(InternalKey internalKey, Object key) {
        return hLowerBound(internalKey, key).mapString().values().iterator().next();
    }

    @Override
    public Response hDel(InternalKey internalKey, Object key) {
        return ssdb.hdel(internalKey.toString(), key);
    }

    @Override
    @Async
    public Response hDel(InternalKey internalKey, Object fromKey, Object endKey) {
        return ssdb.multi_hdel(internalKey.toString(), hScanKeys(internalKey, fromKey, endKey));
    }

    @Override
    @Async
    public Response hClear(InternalKey internalKey) {
        return ssdb.hclear(internalKey.toString());
    }

    @Override
    public Response sSet(Object name, Object element) {
        return ssdb.hset(name, element, new byte[0]);
    }

    @Override
    public Set<String> sGetAll(Object name) {
        return ssdb.hgetall(name).mapString().keySet();
    }

    @Override
    public List<String> sScan(Object name, Object startElement, Object endElement) {
        return ssdb.hkeys(name, startElement, endElement, KVStoreConfig.getScanNumber()).listString();
    }

    @Override
    @Async
    public Response sClear(Object name) {
        return ssdb.hclear(name);
    }

    @Override
    @Async
    public Response sDel(Object name, Object element) {
        return ssdb.hdel(name, element);
    }
}
