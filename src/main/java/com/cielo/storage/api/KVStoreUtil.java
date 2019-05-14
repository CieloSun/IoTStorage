package com.cielo.storage.api;

import com.alibaba.fastjson.parser.Feature;
import com.cielo.storage.model.InternalKey;
import org.nutz.ssdb4j.spi.Response;
import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

public interface KVStoreUtil {
    //设置一个基本类型值
    Response setVal(Object key, Object val);

    //设置JSON对象
    Response set(Object key, Object val);

    //设置JSON对象并增加时限
    Response setx(Object key, Object val, int ttl);

    //原始get方法用于获取基本值
    Response get(Object key);

    //获取一个对象并反序列化
    <T> T get(Object key, Class<T> clazz, Feature... features);

    //基于首尾字符串获取原始MapString
    Map<String, String> scan(Object fromKey, Object endKey);

    //基于前缀获取原始MapString
    Map<String, String> scan(Object prefix);

    //获取前缀的key
    List<String> scanKeys(Object prefix);

    //基于首尾字符串获取key
    List<String> scanKeys(Object fromKey, Object endKey);

    //基于前缀获取值
    String scanValues(Object prefix);

    //基于首尾字符串获取值
    String scanValues(Object fromKey, Object endKey);

    //基于前缀获取值并反序列化
    <T> List<T> scanValues(Object prefix, Class<T> clazz);

    //基于首尾字符串获取值并反序列化
    <T> List<T> scanValues(Object fromKey, Object endKey, Class<T> clazz);

    Response lowerBoundKey(Object key);

    String lowerBoundVal(Object key);

    Response lowerBound(Object key);

    //获取计数，数量不超过配置中的scanNumber
    int count(Object prefix);

    //设置超时时间
    Response expire(Object key, int ttl);

    //删除一个key
    Response del(Object key);

    //基于前缀删除多个key，异步
    @Async
    Future<Response> multiDel(Object prefix);

    @Async
    Future<Response> multiDel(Object prefix, Object fromKey, Object endKey);

    //删除多个key，异步
    @Async
    Future<Response> multiDel(Object[] keys);

    //判断一个key是否存在
    boolean exists(Object key);

    //某key值自增1
    Response incr(Object key);

    //某key值自增
    Response incr(Object key, int val);

    //某key值自减1
    Response decr(Object key);

    //某key值自减
    Response decr(Object key, int val);

    Response hSetVal(InternalKey internalKey, Object key, Object val);

    Response hSet(InternalKey internalKey, Object key, Object val);

    Response hMultiSet(InternalKey internalKey, Map map);

    Response hGet(InternalKey internalKey, Object key);

    <T> T hGet(InternalKey internalKey, Object key, Class<T> clazz, Feature... features);

    Response hMultiGet(InternalKey internalKey, Object... keys);

    <T> List<T> hMultiGet(Class<T> clazz, InternalKey internalKey, Object... keys);

    Response hGetAll(InternalKey internalKey);

    <T> List<T> hGetAll(InternalKey internalKey, Class<T> clazz);

    List<String> hGetAllKeys(InternalKey internalKey);

    Integer hSize(InternalKey internalKey);

    boolean hExists(InternalKey internalKey, Object key);

    List<String> hScanName(Object fromName, Object endName);

    List<String> hScanName(Object prefix);

    List<String> hScanName(InternalKey internalKey);

    List<String> hScanKeys(InternalKey internalKey, Object fromKey, Object endKey);

    List<String> hScanKeys(InternalKey internalKey, Object prefix);

    Map<String, String> hScan(InternalKey internalKey, Object fromKey, Object endKey);

    Map<String, String> hScan(InternalKey internalKey, Object prefix);

    <K,V> Map<K, V> hScan(InternalKey internalKey, Object fromKey, Object endKey, Class<K> kType, Class<V> vType);

    <K,V> Map<K, V> hScan(InternalKey internalKey, Object prefix, Class<K> kType, Class<V> vType);

    String hScanValues(InternalKey internalKey, Object fromKey, Object endKey);

    String hScanValues(InternalKey internalKey, Object prefix);

    <T> List<T> hScanValues(InternalKey internalKey, Object fromKey, Object endKey, Class<T> clazz);

    <T> List<T> hScanValues(InternalKey internalKey, Object prefix, Class<T> clazz);

    Response hLowerBoundKey(InternalKey internalKey, Object key);

    Response hLowerBound(InternalKey internalKey, Object key);

    String hLowerBoundVal(InternalKey internalKey, Object key);

    Response hDel(InternalKey internalKey, Object key);

    @Async
    Future<Response> hDel(InternalKey internalKey, Object fromKey, Object endKey);

    @Async
    Future<Response> hClear(InternalKey internalKey);

    Response sSet(Object name, Object element);

    Set<String> sGetAll(Object name);

    List<String> sScan(Object name, Object startElement, Object endElement);

    @Async
    Future<Response> sClear(Object name);

    @Async
    Future<Response> sDel(Object name, Object element);
}
