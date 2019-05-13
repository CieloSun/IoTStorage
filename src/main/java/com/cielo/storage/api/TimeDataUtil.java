package com.cielo.storage.api;

import java.util.List;
import java.util.Map;

public interface TimeDataUtil {
    //便于用户打包多个变量
    String[] tags(String... args);

    String[] tags(String primeType, String... args);

    //根据tags存储数据
    void set(String[] tags, Object val);

    //根据tags存储数据，并手动指定primeType
    void set(String[] tags, String primeType, Object val);

    //根据primeType和primeTag查找数据
    List<String> searchKeys(String primeType, String primeTag);

    //根据primeType和查找值查找数据
    List<String> searchKeys(String primeType, String searchType, String searchTag);

    //根据tags获取可能归档的hashMap中具体某条数据
    <T> T get(String[] tags, Long timestamp, Class<T> clazz) throws Exception;

    //根据tags获取可能归档的hashMap中具体某条数据，并手动指定primeType
    <T> T get(String[] tags, String primeType, Long timestamp, Class<T> clazz) throws Exception;

    //根据tags获取获取可能归档的hashMap中某段数据
    <T> Map<Object, T> get(String[] tags, Long startTime, Long endTime, Class<T> clazz) throws Exception;

    //根据tags获取获取可能归档的hashMap中某段数据，并手动指定primeType
    <T> Map<Object, T> get(String[] tags, String primeType, Long startTime, Long endTime, Class<T> clazz) throws Exception;

    //根据tags获取获取最新一条数据
    <T> T getLatest(String[] tags, Class<T> clazz) throws Exception;

    //根据tags获取获取最新一条数据，并手动指定primeType
    <T> T getLatest(String[] tags, String primeType, Class<T> clazz) throws Exception;

    //根据tags获取删除数据
    void del(String[] tags);

    void del(String[] tags, Long startTime, Long endTime);

    //根据tags删除数据，并手动指定primeType
    void del(String[] tags, String primeType);

    void del(String[] tags, String primeType, Long startTime, Long endTime);

    void archiveJob();
}
