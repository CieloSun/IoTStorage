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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
class TimeDataUtilImpl implements TimeDataUtil {
    private static final String LATEST_ARCHIVE = "latest_file";
    private static final String LATEST_VAL = "latest_val";
    private static final String LATEST_TIME = "latest_time";
    private Logger logger = LoggerFactory.getLogger(this.getClass());
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

    private Long latestDBTime(InternalKey internalKey) {
        Response response = kvStoreUtil.hGet(internalKey, LATEST_ARCHIVE);
        if (response.notFound()) return 0L;
        return response.asLong();
    }

    private Long latestCacheTime(InternalKey internalKey) {
        Object val = cacheUtil.getVal(internalKey, LATEST_TIME);
        if (val instanceof Long) return (Long) val;
        return 0L;
    }

    private void switchByPrime(String[] tags, String primeType) {
        for (int i = 0; i < tags.length; i++) {
            if (tags[i].startsWith(primeType)) {
                String tag = tags[i];
                tags[i] = tags[0];
                tags[0] = tag;
                break;
            }
        }
    }

    @Override
    public String[] tags(String... args) {
        return args;
    }

    @Override
    public String[] tags(String primeType, String... args) {
        switchByPrime(args, primeType);
        return args;
    }

    private Map<Object, Object> packageMap(Object val) {
        val = JSON.toJSONString(val);
        Map<Object, Object> map = new HashMap<>();
        long timestamp = System.currentTimeMillis();
        map.put(timestamp, val);
        map.put(LATEST_VAL, val);
        map.put(LATEST_TIME, timestamp);
        return map;
    }

    @Override
    public void set(String[] tags, Object val) {
        cacheUtil.multiSet(new InternalKey(tags), packageMap(val));
    }

    @Override
    public void setSync(String[] tags, Object val) {
        kvStoreUtil.hMultiSet(new InternalKey(tags), packageMap(val));
    }

    @Override
    public void set(String[] tags, String primeType, Object val) {
        switchByPrime(tags, primeType);
        set(tags, val);
    }

    @Override
    public void setSync(String[] tags, String primeType, Object val) {
        switchByPrime(tags, primeType);
        setSync(tags, val);
    }

    @Override
    public List<String> searchKeys(String primeType, String primeTag) {
        return kvStoreUtil.hScanName(primeType + "/" + primeTag);
    }

    @Override
    public List<String> searchKeys(String primeType, String searchType, String searchTag) {
        if (searchType.equals(primeType)) return searchKeys(searchType, searchTag);
        return StreamProxy.stream(kvStoreUtil.hScanName(primeType + "/")).filter(s -> s.contains(searchType + "/" + searchTag)).collect(Collectors.toList());
    }

    @Override
    public <T> T get(String[] tags, Long timestamp, Class<T> clazz) throws Exception {
        InternalKey internalKey = new InternalKey(tags);
        T t = cacheUtil.get(internalKey, timestamp, clazz);
        if (t != null) return t;
        return getFromDB(internalKey, timestamp, clazz);
    }

    @Override
    public <T> T get(String[] tags, String primeType, Long timestamp, Class<T> clazz) throws Exception {
        switchByPrime(tags, primeType);
        return get(tags, timestamp, clazz);
    }

    @Override
    public <T> T getSync(String[] tags, Long timestamp, Class<T> clazz) throws Exception {
        return getFromDB(new InternalKey(tags), timestamp, clazz);
    }

    @Override
    public <T> T getSync(String[] tags, String primeType, Long timestamp, Class<T> clazz) throws Exception {
        switchByPrime(tags, primeType);
        return getSync(tags, timestamp, clazz);
    }

    private <T> T getFromDB(InternalKey internalKey, Long timestamp, Class<T> clazz) throws Exception {
        return persistUtil.downloadMap(kvStoreUtil.hLowerBoundVal(internalKey, timestamp), Long.class, clazz).get().get(timestamp);
    }

    @Override
    public <T> Map<Long, T> get(String[] tags, Long startTime, Long endTime, Class<T> clazz) throws Exception {
        InternalKey internalKey = new InternalKey(tags);
        //根据输入确定起始，最终位置
        final long startKey;
        final long endKey;
        startKey = startTime == null ? 0L : startTime;
        if (endTime == null) {
            endTime = System.currentTimeMillis();
            endKey = endTime;
        } else {
            //大于终止时间的第一个文件对应的key，一次SSDB访问
            Response response = kvStoreUtil.hLowerBoundKey(internalKey, endTime);
            if (response.notFound()) endKey = endTime;//如果没有比endTime更新的文件
            else endKey = response.asLong();
        }
        //结果表
        Map<Long, T> result = new HashMap<>();
        //获取SSDB上次同步时间，一次SSDB访问
        Long latestSyncArchiveTime = latestDBTime(internalKey);
        //如果结束key大于SSDB上次同步时间，则访问本地缓存
        if (endKey >= latestSyncArchiveTime) result.putAll(cacheUtil.scan(internalKey, startKey, endKey, clazz));
        //开始key小于SSDB上次同步时间
        if (startKey < latestSyncArchiveTime) {
            //获取范围内的所有key，一次SSDB访问
            List<String> values = (List<String>) kvStoreUtil.hScan(internalKey, startKey, endKey).values();
            if (values.size() != 0) {
                //特殊情况，仅一个文件
                if (values.size() == 1) {
                    //下载文件,一次FastDFS访问
                    Map<Long, T> fileMap = persistUtil.downloadMap(values.get(0), Long.class, clazz).get();
                    //筛选文件中不小于startKey，不大于endKey的数据
                    StreamProxy.stream(fileMap.entrySet()).filter(e -> e.getKey() >= startKey && e.getKey() <= endKey).forEach(e -> result.put(e.getKey(), e.getValue()));
                } else {
                    //异步下载第一个文件，一次FastDFS访问
                    CompletableFuture<Map<Long, T>> firstFileFuture = persistUtil.downloadMap(values.get(0), Long.class, clazz);
                    //异步下载最后一个文件，一次FastDFS访问
                    CompletableFuture<Map<Long, T>> lastFileFuture = persistUtil.downloadMap(values.get(values.size() - 1), Long.class, clazz);
                    //如果文件数大于2，获取第二到倒数第二个文件中全部数据
                    if (values.size() > 2)
                        //数据量大于1时使用parallelStream,否则在当前线程执行，N次FastDFS访问
                        StreamProxy.stream(1, values.subList(1, values.size() - 1)).map(Try.of(path -> persistUtil.downloadMap(path, Long.class, clazz))).map(Try.of(CompletableFuture::get)).forEach(result::putAll);
                    //第一个文件中筛选不小于startKey的数据
                    StreamProxy.stream(firstFileFuture.get().entrySet()).filter(e -> e.getKey() >= startKey).forEach(e -> result.put(e.getKey(), e.getValue()));
                    //最后一个文件中筛选不大于endKey的数据
                    StreamProxy.stream(lastFileFuture.get().entrySet()).filter(e -> e.getKey() <= endKey).forEach(e -> result.put(e.getKey(), e.getValue()));
                }
            }
        }
        //返回结果
        return result;
    }

    @Override
    public <T> Map<Long, T> get(String[] tags, String primeType, Long startTime, Long endTime, Class<T> clazz) throws Exception {
        switchByPrime(tags, primeType);
        return get(tags, startTime, endTime, clazz);
    }

    @Override
    public <T> Map<Long, T> getSync(String[] tags, Long startTime, Long endTime, Class<T> clazz) {
        //根据输入确定起始，最终位置
        final long startKey = startTime == null ? 0L : startTime;
        final long endKey = endTime == null ? System.currentTimeMillis() : endTime;
        return kvStoreUtil.hScan(new InternalKey(tags), startKey, endKey, Long.class, clazz);
    }

    @Override
    public <T> Map<Long, T> getSync(String[] tags, String primeType, Long startTime, Long endTime, Class<T> clazz) {
        switchByPrime(tags, primeType);
        return getSync(tags, startTime, endTime, clazz);
    }

    @Override
    public <T> T getLatest(String[] tags, Class<T> clazz) throws Exception {
        InternalKey internalKey = new InternalKey(tags);
        Long latestDBTime = latestDBTime(internalKey);
        if (latestCacheTime(internalKey) > latestDBTime)
            return cacheUtil.get(internalKey, LATEST_VAL, clazz);
        return getFromDB(internalKey, latestDBTime, clazz);
    }

    @Override
    public <T> T getLatest(String[] tags, String primeType, Class<T> clazz) throws Exception {
        switchByPrime(tags, primeType);
        return getLatest(tags, clazz);
    }

    @Override
    public void del(String[] tags) {
        InternalKey internalKey = new InternalKey(tags);
        cacheUtil.clear(internalKey);
        if (timeDataConfig.getDeleteValueTogether())
            persistUtil.multiDelete(kvStoreUtil.hGetAll(internalKey).mapString().values());
        kvStoreUtil.hClear(internalKey);
    }

    @Override
    public void del(String[] tags, String primeType) {
        switchByPrime(tags, primeType);
        del(tags);
    }

    @Override
    public void del(String[] tags, Long startTime, Long endTime) {
        InternalKey internalKey = new InternalKey(tags);
        cacheUtil.delete(internalKey, startTime, endTime);
        if (timeDataConfig.getDeleteValueTogether())
            persistUtil.multiDelete(kvStoreUtil.hScan(internalKey, startTime, endTime).values());
        kvStoreUtil.hDel(internalKey, startTime, endTime);
    }

    @Override
    public void del(String[] tags, String primeType, Long startTime, Long endTime) {
        switchByPrime(tags, primeType);
        del(tags, startTime, endTime);
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
            Map<Long, Object> valueMap = cacheUtil.scan(internalKey, latestLocalArchiveTime(internalKey), archiveTime);
            if (valueMap.size() != 0) {
                CompletableFuture<String> uploadTask;
                String content = JSONUtil.toMapJSON(valueMap);
                try {
                    if (timeDataConfig.getSaveKeyInValue()) {
                        Map<String, String> infos = new HashMap<>();
                        infos.put("internalKey", internalKey.toString());
                        infos.put("timestamp", archiveTime.toString());
                        uploadTask = persistUtil.upload(content, infos);
                    } else uploadTask = persistUtil.upload(content);
                    if (StringUtils.hasText(uploadTask.get())) {
                        cacheUtil.set(internalKey, LATEST_ARCHIVE, archiveTime);
                        Map<Object, Object> map = new HashMap<>();
                        map.put(LATEST_ARCHIVE, archiveTime);
                        map.put(archiveTime, uploadTask);
                        kvStoreUtil.hMultiSet(internalKey, map);
                        logger.info(internalKey + ":" + uploadTask + " has archived on " + new Date(archiveTime));
                    } else logger.info(internalKey + " cannot archive.");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else logger.info("Nothing needs to archive.");
        });
    }
}
