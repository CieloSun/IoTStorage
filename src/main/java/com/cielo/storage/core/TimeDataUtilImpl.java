package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.CacheUtil;
import com.cielo.storage.api.KVStoreUtil;
import com.cielo.storage.api.PersistUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.config.TimeDataConfig;
import com.cielo.storage.model.InternalKey;
import com.cielo.storage.tool.JSONUtil;
import com.cielo.storage.tool.StreamProxy;
import com.cielo.storage.tool.Try;
import org.nutz.ssdb4j.spi.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
class TimeDataUtilImpl implements TimeDataUtil {
    private static final String LATEST_ARCHIVE = "latest_file";
    private static final String LATEST_VAL = "latest_val";
    private static final String LATEST_TIME = "latest_time";
    Logger logger = LoggerFactory.getLogger(this.getClass());
    private final PersistUtil persistUtil;
    private final KVStoreUtil kvStoreUtil;
    private final CacheUtil cacheUtil;
    private final TimeDataConfig timeDataConfig;

    @Autowired
    public TimeDataUtilImpl(PersistUtil persistUtil, KVStoreUtil kvStoreUtil, CacheUtil cacheUtil, TimeDataConfig timeDataConfig) {
        this.persistUtil = persistUtil;
        this.kvStoreUtil = kvStoreUtil;
        this.cacheUtil = cacheUtil;
        this.timeDataConfig = timeDataConfig;
    }

    private Long latestSyncArchiveTime(InternalKey internalKey) {
        Response response = kvStoreUtil.hGet(internalKey, LATEST_ARCHIVE);
        if (response.notFound()) return 0L;
        return response.asLong();
    }

    private Long latestLocalSaveTime(InternalKey internalKey) {
        Object val = cacheUtil.getVal(internalKey, LATEST_TIME);
        if (val instanceof Long) return (Long) val;
        return 0L;
    }

    @Override
    public void set(InternalKey internalKey, Object val) {
        val = JSON.toJSONString(val);
        Map<Object, Object> map = new HashMap<>();
        long timestamp = System.currentTimeMillis();
        map.put(timestamp, val);
        map.put(LATEST_VAL, val);
        map.put(LATEST_TIME, timestamp);
        cacheUtil.multiSet(internalKey, map);
    }

    @Override
    public List<String> getInternalKeys(String primeType, String primeTag) {
        return getInternalKeys(primeType + "/" + primeTag);
    }

    @Override
    public List<String> getInternalKeys(String primeType, String searchType, String searchTag) {
        return StreamProxy.stream(getInternalKeys(primeType + "/")).filter(s -> s.contains(searchType + "/" + searchTag)).collect(Collectors.toList());
    }

    @Override
    public List<String> getInternalKeys(String internalKeyPrefix) {
        return kvStoreUtil.hScanName(internalKeyPrefix);
    }

    //获取可能归档的hashMap中具体某条数据
    @Override
    public <T> T get(InternalKey internalKey, Long timestamp, Class<T> clazz) throws Exception {
        T t = cacheUtil.get(internalKey, timestamp, clazz);
        if (t != null) return t;
        return getSync(internalKey, timestamp, clazz);
    }

    private <T> T getSync(InternalKey internalKey, Long timestamp, Class<T> clazz) throws Exception {
        return (T) persistUtil.downloadMap(kvStoreUtil.hLowerBoundVal(internalKey, timestamp)).get(timestamp);
    }

    //获取可能归档的hashMap中某段数据,由于Lambda要求所在方法可重入，因而拆分
    @Override
    public <T> Map<Object, T> get(InternalKey internalKey, Long startTime, Long endTime, Class<T> clazz) {
        if (startTime == null) startTime = 0L;
        if (endTime == null) endTime = System.currentTimeMillis();
        return getMap(internalKey, startTime, endTime, clazz);
    }

    private <T> Map<Object, T> getMap(InternalKey internalKey, Long startTime, Long endTime, Class<T> clazz) {
        Map<Object, T> map = new HashMap<>();
        Long latestSyncArchiveTime = latestSyncArchiveTime(internalKey);
        if (endTime >= latestSyncArchiveTime) map.putAll(cacheUtil.scan(internalKey, startTime, endTime, clazz));
        if (startTime < latestSyncArchiveTime)
            StreamProxy.parallelStream(kvStoreUtil.hScan(internalKey, startTime, kvStoreUtil.hLowerBoundKey(internalKey, endTime).asLong()).values()).map(Try.of(persistUtil::downloadMap)).forEach(map::putAll);
        StreamProxy.stream(map.keySet()).filter(key -> (Long) key < startTime || (Long) key > endTime).forEach(map::remove);
        return map;
    }

    @Override
    public <T> T getLatest(InternalKey internalKey, Class<T> clazz) throws Exception {
        Long latestSyncArchiveTime = latestSyncArchiveTime(internalKey);
        if (latestLocalSaveTime(internalKey) > latestSyncArchiveTime)
            return cacheUtil.get(internalKey, LATEST_VAL, clazz);
        return getSync(internalKey, latestSyncArchiveTime, clazz);
    }

    @Override
    public void del(InternalKey internalKey) {
        cacheUtil.clear(internalKey);
        if (timeDataConfig.getDeleteValueTogether())
            persistUtil.multiDelete(kvStoreUtil.hGetAll(internalKey).mapString().values());
        kvStoreUtil.hClear(internalKey);
    }

    @Override
    public void del(InternalKey internalKey, Long startTime, Long endTime) {
        cacheUtil.delete(internalKey, startTime, endTime);
        if (timeDataConfig.getDeleteValueTogether())
            persistUtil.multiDelete(kvStoreUtil.hScan(internalKey, startTime, endTime).values());
        kvStoreUtil.hDel(internalKey, startTime, endTime);
    }

    private Long latestLocalArchiveTime(InternalKey internalKey) {
        Object val = cacheUtil.getVal(internalKey, LATEST_ARCHIVE);
        if (val instanceof Long) return (Long) val;
        return 0L;
    }

    //hashMap中数据归档,hashMap中key为时间戳，val为JSON对象
    @Override
    public void archiveJob() {
        StreamProxy.parallelStream(cacheUtil.allInternalKeys()).forEach(internalKey -> {
            Long archiveTime = System.currentTimeMillis();
            Map valueMap = cacheUtil.scan(internalKey, latestLocalArchiveTime(internalKey), archiveTime);
            if (valueMap.size() != 0) {
                String fileId;
                String content = JSONUtil.toMapJSON(valueMap);
                if (timeDataConfig.getSaveKeyInValue()) {
                    Map<String, String> infos = new HashMap<>();
                    infos.put("internalKey", internalKey.toString());
                    infos.put("timestamp", archiveTime.toString());
                    fileId = persistUtil.upload(content, infos);
                } else fileId = persistUtil.upload(content);
                if (StringUtils.hasText(fileId)) {
                    cacheUtil.set(internalKey, LATEST_ARCHIVE, archiveTime);
                    Map<Object, Object> map = new HashMap<>();
                    map.put(LATEST_ARCHIVE, archiveTime);
                    map.put(archiveTime, fileId);
                    kvStoreUtil.hMultiSet(internalKey, map);
                    logger.info(internalKey + ":" + fileId + " has archived on " + new Date(archiveTime));
                } else logger.info(internalKey + " cannot archive.");
            } else logger.info("Nothing needs to archive.");
        });
    }
}
