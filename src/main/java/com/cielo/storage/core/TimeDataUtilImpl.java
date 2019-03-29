package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.CacheUtil;
import com.cielo.storage.api.KVStoreUtil;
import com.cielo.storage.api.PersistUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.config.TimeDataConfig;
import com.cielo.storage.model.DataTag;
import com.cielo.storage.tool.JSONUtil;
import com.cielo.storage.tool.StreamProxy;
import com.cielo.storage.tool.Try;
import org.nutz.ssdb4j.spi.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
class TimeDataUtilImpl implements TimeDataUtil {
    private static final String LATEST_ARCHIVE = "latest_file";
    private static final String LATEST_VAL = "latest_val";
    private static final String LATEST_TIME = "latest_time";
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private PersistUtil persistUtil;
    @Autowired
    private KVStoreUtil kvStoreUtil;
    @Autowired
    private CacheUtil cacheUtil;
    @Autowired
    private TimeDataConfig timeDataConfig;

    private Long latestSyncArchiveTime(DataTag dataTag) {
        Response response = kvStoreUtil.hGet(dataTag, LATEST_ARCHIVE);
        if (response.notFound()) return 0L;
        return response.asLong();
    }

    private Long latestLocalArchiveTime(DataTag dataTag) {
        Object val = cacheUtil.getVal(dataTag, LATEST_ARCHIVE);
        if (val instanceof Long) return (Long) val;
        return 0l;
    }

    private Long latestLocalSaveTime(DataTag dataTag) {
        Object val = cacheUtil.getVal(dataTag, LATEST_TIME);
        if (val instanceof Long) return (Long) val;
        return 0l;
    }

    @Override
    public void set(DataTag dataTag, Object val) {
        val = JSON.toJSONString(val);
        Map<Object, Object> map = new HashMap<>();
        long timestamp = System.currentTimeMillis();
        map.put(timestamp, val);
        map.put(LATEST_VAL, val);
        map.put(LATEST_TIME, timestamp);
        cacheUtil.multiSet(dataTag, map);
    }

    //获取可能归档的hashMap中具体某条数据
    @Override
    public <T> T get(DataTag dataTag, Long timestamp, Class<T> clazz) throws Exception {
        T t = cacheUtil.get(dataTag, timestamp, clazz);
        if (t != null) return t;
        return (T) persistUtil.downloadMap(kvStoreUtil.hLowerBoundVal(dataTag, timestamp)).get(timestamp);
    }

    //获取可能归档的hashMap中某段数据,由于Lambda要求所在方法可重入，因而拆分
    @Override
    public <T> Map<Object, T> get(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz) {
        if (startTime == null) startTime = 0L;
        if (endTime == null) endTime = System.currentTimeMillis();
        return getMap(dataTag, startTime, endTime, clazz);
    }

    private <T> Map<Object, T> getMap(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz) {
        Map<Object, T> map = new HashMap<>();
        Long latestSyncArchiveTime = latestSyncArchiveTime(dataTag);
        if (endTime >= latestSyncArchiveTime) map.putAll(cacheUtil.scan(dataTag, startTime, endTime, clazz));
        if (startTime < latestSyncArchiveTime)
            StreamProxy.stream(kvStoreUtil.hScan(dataTag, startTime, kvStoreUtil.hLowerBoundKey(dataTag, endTime).asLong()).values()).map(Try.of(fileId -> persistUtil.downloadMap(fileId))).forEach(map::putAll);
        StreamProxy.stream(map.keySet()).filter(key -> (Long) key < startTime || (Long) key > endTime).forEach(map::remove);
        return map;
    }

    @Override
    public <T> T getLatest(DataTag dataTag, Class<T> clazz) throws Exception {
        Long latestSyncArchiveTime = latestSyncArchiveTime(dataTag);
        long latestLocalTime = latestLocalSaveTime(dataTag);
        if (latestLocalTime > latestSyncArchiveTime) return cacheUtil.get(dataTag, LATEST_VAL, clazz);
        else return get(dataTag, latestSyncArchiveTime, clazz);
    }

    @Override
    public void del(DataTag dataTag) {
        cacheUtil.clear(dataTag);
        if (timeDataConfig.getDeleteValueTogether())
            persistUtil.multiDelete(kvStoreUtil.hGetAll(dataTag).mapString().values());
        kvStoreUtil.hClear(dataTag);
    }

    @Override
    public void del(DataTag dataTag, Long startTime, Long endTime) {
        cacheUtil.delete(dataTag, startTime, endTime);
        if (timeDataConfig.getDeleteValueTogether())
            persistUtil.multiDelete(kvStoreUtil.hScan(dataTag, startTime, endTime).values());
        kvStoreUtil.hDel(dataTag, startTime, endTime);
    }

    private Set<DataTag> configTags() {
        Set<DataTag> tagSet = new HashSet<>();
        StreamProxy.stream(timeDataConfig.getArchiveTags()).forEach(tagString -> StreamProxy.stream(cacheUtil.searchCacheNames(tagString)).map(DataTag::new).forEach(tagSet::add));
        return tagSet;
    }

    //hashMap中数据归档,hashMap中key为时间戳，val为JSON对象
    @Override
    public void archiveJob() {
        StreamProxy.stream(configTags()).forEach(tag -> {
            Long archiveTime = System.currentTimeMillis();
            Map<String, String> valueMap = cacheUtil.scan(tag, latestLocalArchiveTime(tag), archiveTime);
            if (valueMap.size() != 0) {
                String fileId;
                String content = JSONUtil.toMapJSON(valueMap);
                if (timeDataConfig.getSaveKeyInValue()) {
                    Map<String, String> infos = new HashMap<>();
                    infos.put("tag", tag.toString());
                    infos.put("timestamp", archiveTime.toString());
                    fileId = persistUtil.upload(content, infos);
                } else fileId = persistUtil.upload(content);
                if (StringUtils.hasText(fileId)) {
                    cacheUtil.set(tag, LATEST_ARCHIVE, archiveTime);
                    Map<Object, Object> syncMap = new HashMap<>();
                    syncMap.put(LATEST_ARCHIVE, archiveTime);
                    syncMap.put(archiveTime, fileId);
                    kvStoreUtil.hMultiSet(tag, syncMap);
                    logger.info(tag + ":" + fileId + " has archived on " + new Date(archiveTime));
                } else logger.info(tag + " cannot archive.");
            } else logger.info("Nothing needs to archive.");
        });
    }
}
