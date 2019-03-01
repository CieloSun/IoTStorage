package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.CacheUtil;
import com.cielo.storage.config.TimeDataConfig;
import com.cielo.storage.model.DataTag;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.ehcache.EhCacheCacheManager;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
class CacheUtilImpl implements CacheUtil {
    @Resource
    private EhCacheCacheManager ehCacheCacheManager;
    @Autowired
    private TimeDataConfig timeDataConfig;

    //利用不同的cache来实现类hashMap结构
    private Cache getCache(DataTag dataTag) {
        String cacheName = dataTag.toString();
        CacheManager cacheManager = ehCacheCacheManager.getCacheManager();
        Cache cache = cacheManager != null ? cacheManager.getCache(cacheName) : null;
        if (cache == null) {
            Objects.requireNonNull(cacheManager).addCacheIfAbsent(new Cache(cacheName, 0, false, true, Integer.MAX_VALUE, timeDataConfig.getClearInterval()));
            cache = cacheManager.getCache(cacheName);
        }
        return cache;
    }

    private boolean rangeFilter(Long key, Long startKey, Long endKey) {
        return key >= startKey && key <= endKey;
    }

    private List<Long> getLongKeys(Cache cache) {
        return cache.getKeys().parallelStream().filter(key -> key instanceof Long).mapToLong(key -> (Long) key).boxed().collect(Collectors.toList());
    }

    @Override
    public void set(DataTag cacheName, Object key, Object val) {
        getCache(cacheName).put(new Element(key, val));
    }

    @Override
    public void multiSet(DataTag cacheName, Map<Object, Object> map) {
        getCache(cacheName).putAll(map.entrySet().parallelStream().map(entry -> new Element(entry.getKey(), entry.getValue())).collect(Collectors.toList()));
    }

    @Override
    public void delete(DataTag cacheName, Long startKey, Long endKey) {
        Cache cache = getCache(cacheName);
        List<Long> keys = getLongKeys(cache);
        cache.removeAll(keys.parallelStream().filter(key -> rangeFilter(key, startKey, endKey)).collect(Collectors.toList()));
    }

    @Override
    public void clear(DataTag cacheName) {
        Objects.requireNonNull(ehCacheCacheManager.getCacheManager()).removeCache(cacheName.toString());
    }

    @Override
    public Object getVal(DataTag cacheName, Object key) {
        Element element = getCache(cacheName).get(key);
        if (element == null) return null;
        return element.getObjectValue();
    }

    @Override
    public <T> T get(DataTag cacheName, Object key, Class<T> clazz) {
        return JSON.parseObject(getCache(cacheName).get(key).getObjectValue().toString(), clazz);
    }

    @Override
    public <T> Map<Object, T> scan(DataTag cacheName, Long startKey, Long endKey, Class<T> clazz) {
        Cache cache = getCache(cacheName);
        List<Long> keys = getLongKeys(cache);
        return keys.parallelStream().filter(key -> rangeFilter(key, startKey, endKey)).collect(Collectors.toMap(Function.identity(), key -> JSON.parseObject(cache.get(key).getObjectValue().toString(), clazz), (a, b) -> b));
    }

    @Override
    public Map scan(DataTag cacheName, Long startKey, Long endKey) {
        Cache cache = getCache(cacheName);
        List<Long> keys = getLongKeys(cache);
        return keys.parallelStream().filter(key -> rangeFilter(key, startKey, endKey)).collect(Collectors.toMap(Function.identity(), key -> cache.get(key).getObjectValue(), (a, b) -> b));
    }

    @Override
    public List<String> searchCacheNames(String prefix) {
        return Arrays.asList(Objects.requireNonNull(ehCacheCacheManager.getCacheManager()).getCacheNames()).parallelStream().filter(tag -> tag.contains(prefix)).collect(Collectors.toList());
    }

    @Override
    public int size(DataTag dataTag) {
        return getCache(dataTag).getSize();
    }
}
