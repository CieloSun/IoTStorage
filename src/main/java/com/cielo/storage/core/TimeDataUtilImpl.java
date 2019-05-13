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
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
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

    @Override
    public void set(String[] tags, Object val) {
        set(new InternalKey(tags), val);
    }

    @Override
    public void set(String[] tags, String primeType, Object val) {
        switchByPrime(tags, primeType);
        set(tags, val);
    }

    private void set(InternalKey internalKey, Object val) {
        val = JSON.toJSONString(val);
        Map<Object, Object> map = new HashMap<>();
        long timestamp = System.currentTimeMillis();
        map.put(timestamp, val);
        map.put(LATEST_VAL, val);
        map.put(LATEST_TIME, timestamp);
        cacheUtil.multiSet(internalKey, map);
    }

    @Override
    public List<String> searchKeys(String primeType, String primeTag) {
        return kvStoreUtil.hScanName(primeType + "/" + primeTag);
    }

    @Override
    public List<String> searchKeys(String primeType, String searchType, String searchTag) {
        if (searchType == primeType) return searchKeys(searchType, searchTag);
        return StreamProxy.stream(kvStoreUtil.hScanName(primeType + "/")).filter(s -> s.contains(searchType + "/" + searchTag)).collect(Collectors.toList());
    }

    @Override
    public <T> T get(String[] tags, Long timestamp, Class<T> clazz) throws Exception {
        return get(new InternalKey(tags), timestamp, clazz);
    }

    @Override
    public <T> T get(String[] tags, String primeType, Long timestamp, Class<T> clazz) throws Exception {
        switchByPrime(tags, primeType);
        return get(tags, timestamp, clazz);
    }

    private <T> T get(InternalKey internalKey, Long timestamp, Class<T> clazz) throws Exception {
        T t = cacheUtil.get(internalKey, timestamp, clazz);
        if (t != null) return t;
        return getSync(internalKey, timestamp, clazz);
    }

    private <T> T getSync(InternalKey internalKey, Long timestamp, Class<T> clazz) throws Exception {
        return (T) persistUtil.downloadMap(kvStoreUtil.hLowerBoundVal(internalKey, timestamp)).get(timestamp);
    }

    @Override
    public <T> Map<Object, T> get(String[] tags, Long startTime, Long endTime, Class<T> clazz) throws Exception {
        return get(new InternalKey(tags), startTime, endTime, clazz);
    }

    @Override
    public <T> Map<Object, T> get(String[] tags, String primeType, Long startTime, Long endTime, Class<T> clazz) throws Exception {
        switchByPrime(tags, primeType);
        return get(tags, startTime, endTime, clazz);
    }

    private <T> Map<Object, T> get(InternalKey internalKey, Long startTime, Long endTime, Class<T> clazz) throws Exception {
        //根据输入确定起始，最终位置
        final long startKey;
        final long endKey;
        startKey = startTime == null ? 0L : startTime;
        if (endTime == null) {
            endTime = System.currentTimeMillis();
            endKey = endTime;
        } else {
            Response response = kvStoreUtil.hLowerBoundKey(internalKey, endTime);
            if (response.notFound()) endKey = endTime;//如果没有比endTime更新的文件
            else endKey = response.asLong();//大于终止时间的第一个文件对应的key，一次SSDB访问
        }
        //结果表
        Map<Object, T> result = new HashMap<>();
        //获取SSDB上次同步时间，一次SSDB访问
        Long latestSyncArchiveTime = latestSyncArchiveTime(internalKey);
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
                    Map<Object, T> fileMap = persistUtil.downloadMap(values.get(0));
                    //筛选文件中不小于startKey，不大于endKey的数据
                    StreamProxy.stream(fileMap.entrySet()).filter(e -> (Long) e.getKey() >= startKey && (Long) e.getKey() <= endKey).forEach(e -> result.put(e.getKey(), e.getValue()));
                }
                else if (values.size() > 1) {
                    //异步下载第一个文件，一次FastDFS访问
                    Future<Map<Object, T>> firstFileFuture = asyncDownloadMap(values.get(0));
                    //异步下载最后一个文件，一次FastDFS访问
                    Future<Map<Object, T>> lastFileFuture = asyncDownloadMap(values.get(values.size() - 1));
                    //如果文件数大于2，获取第二到倒数第二个文件中全部数据
                    if (values.size() > 2)
                        //数据量大于1时使用parallelStream,否则在当前线程执行，N次FastDFS访问
                        StreamProxy.stream(1, values.subList(1, values.size() - 1)).map(Try.of(persistUtil::downloadMap)).forEach(result::putAll);
                    //第一个文件中筛选不小于startKey的数据
                    StreamProxy.stream(firstFileFuture.get().entrySet()).filter(e -> (Long) e.getKey() >= startKey).forEach(e -> result.put(e.getKey(), e.getValue()));
                    //最后一个文件中筛选不大于endKey的数据
                    StreamProxy.stream(lastFileFuture.get().entrySet()).filter(e -> (Long) e.getKey() <= endKey).forEach(e -> result.put(e.getKey(), e.getValue()));
                }
            }
        }
        //返回结果
        return result;
    }

    @Async
    protected <T> Future<Map<Object, T>> asyncDownloadMap(String path) throws Exception {
        return new AsyncResult<Map<Object, T>>(persistUtil.downloadMap(path));
    }

    @Override
    public <T> T getLatest(String[] tags, Class<T> clazz) throws Exception {
        return getLatest(new InternalKey(tags), clazz);
    }

    @Override
    public <T> T getLatest(String[] tags, String primeType, Class<T> clazz) throws Exception {
        switchByPrime(tags, primeType);
        return getLatest(tags, clazz);
    }

    private <T> T getLatest(InternalKey internalKey, Class<T> clazz) throws Exception {
        Long latestSyncArchiveTime = latestSyncArchiveTime(internalKey);
        if (latestLocalSaveTime(internalKey) > latestSyncArchiveTime)
            return cacheUtil.get(internalKey, LATEST_VAL, clazz);
        return getSync(internalKey, latestSyncArchiveTime, clazz);
    }

    @Override
    public void del(String[] tags) {
        del(new InternalKey(tags));
    }

    @Override
    public void del(String[] tags, String primeType) {
        switchByPrime(tags, primeType);
        del(tags);
    }

    private void del(InternalKey internalKey) {
        cacheUtil.clear(internalKey);
        if (timeDataConfig.getDeleteValueTogether())
            persistUtil.multiDelete(kvStoreUtil.hGetAll(internalKey).mapString().values());
        kvStoreUtil.hClear(internalKey);
    }

    @Override
    public void del(String[] tags, Long startTime, Long endTime) {
        del(new InternalKey(tags), startTime, endTime);
    }

    @Override
    public void del(String[] tags, String primeType, Long startTime, Long endTime) {
        switchByPrime(tags, primeType);
        del(tags, startTime, endTime);
    }

    private void del(InternalKey internalKey, Long startTime, Long endTime) {
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
