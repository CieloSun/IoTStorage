package com.cielo.storage.api;

import com.alibaba.fastjson.parser.Feature;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.springframework.scheduling.annotation.Async;

import java.util.List;
import java.util.Map;

public interface SSDBUtil {
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

    //基于首尾字符串弹出Map
    Map<String, String> popScan(Object fromKey, Object endKey);

    //基于前缀弹出Map
    Map<String, String> popScan(Object prefix);

    //获取计数，数量不超过配置中的scanNumber
    int count(Object prefix);

    //设置超时时间
    Response expire(Object key, int ttl);

    //删除一个key
    Response del(Object key);

    //基于前缀删除多个key，异步
    Response multiDel(Object prefix);

    //删除多个key，异步
    @Async
    Response multiDel(Object[] keys);

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

    Response hSetVal(String name, Object key, Object val);

    Response hSet(String name, Object key, Object val);

    Response hGet(String name, Object key);

    <T> T hGet(String name, Object key, Class<T> clazz, Feature... features);

    Response multiHGet(String name, Object... keys);

    <T> List<T> multiHGet(Class<T> clazz, String name, Object... keys);

    Response hGetAll(String name);

    <T> List<T> hGetAll(String name, Class<T> clazz);

    List<String> hGetAllKeys(String name);

    Response hPopAll(String name);

    <T> List<T> hPopAll(String name, Class<T> clazz);

    Response hDel(String name, Object key);

    Integer hSize(String name);

    boolean hExists(String name, Object key);

    List<String> hScanName(Object fromName, Object endName);

    List<String> hScanName(Object prefix);

    List<String> hScanKeys(String name, Object fromKey, Object endKey);

    List<String> hScanKeys(String name, Object prefix);

    Map<String, String> hScan(String name, Object fromKey, Object endKey);

    Map<String, String> hScan(String name, Object prefix);

    String hScanValues(String name, Object fromKey, Object endKey);

    String hScanValues(String name, Object prefix);

    <T> List<T> hScanValues(String name, Object fromKey, Object endKey, Class<T> clazz);

    <T> List<T> hScanValues(String name, Object prefix, Class<T> clazz);

    <T> Map<Object, T> hScan(String name, Object fromKey, Object endKey, Class<T> clazz);

    <T> Map<Object, T> hScan(String name, Object prefix, Class<T> clazz);

    @Async
    Response hClear(String name);
}
