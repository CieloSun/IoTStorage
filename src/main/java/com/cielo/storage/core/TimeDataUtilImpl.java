package com.cielo.storage.core;

import com.alibaba.fastjson.JSON;
import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.api.SSDBUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.config.ArchiveConfig;
import com.cielo.storage.model.DataTag;
import com.cielo.storage.tool.CollectionUtil;
import com.cielo.storage.tool.JSONUtil;
import com.cielo.storage.tool.Try;
import org.nutz.ssdb4j.spi.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
class TimeDataUtilImpl implements TimeDataUtil {
    private static final DataTag LATEST_ARCHIVE = new DataTag("latest_file");
    private static final String LATEST_VAL = "latest_val";
    private static final String LATEST_TIME = "latest_time";
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSUtil fdfsUtil;
    @Autowired
    private SSDBUtil ssdbSync;
    @Autowired
    @Qualifier("ssdb-local")
    private SSDBUtil ssdbLocal;
    @Autowired
    private ArchiveConfig archiveConfig;

    //获取ssdb-sync中最新归档时间
    private Long latestSyncArchiveTime(DataTag dataTag) {
        Response response = ssdbSync.hGet(dataTag, LATEST_ARCHIVE);
        if (response.notFound()) return 0L;
        return response.asLong();
    }

    //获取ssdb-local中最新归档时间
    private Long latestLocalArchiveTime(DataTag dataTag) {
        Response response = ssdbLocal.hGet(dataTag, LATEST_ARCHIVE);
        if (response.notFound()) return 0L;
        return response.asLong();
    }

    @Override
    public void set(DataTag dataTag, Object val) {
        val = JSON.toJSONString(val);
        Map map = new HashMap();
        long timestamp = System.currentTimeMillis();
        map.put(timestamp, val);
        map.put(LATEST_VAL, val);
        map.put(LATEST_TIME, timestamp);
        ssdbLocal.hMultiSet(dataTag, map);
    }

    //获取可能归档的hashMap中具体某条数据
    @Override
    public <T> T get(DataTag dataTag, Long timestamp, Class<T> clazz) throws Exception {
        T t = ssdbLocal.hGet(dataTag, timestamp, clazz);
        if (t != null) return t;
        Map<String, String> files = ssdbSync.hScan(dataTag, timestamp, timestamp + archiveConfig.getArchiveInterval());
        return fdfsUtil.downloadMap(files.get(CollectionUtil.lowerBoundVal(CollectionUtil.parseLongList(files.keySet()), timestamp)), clazz).get(timestamp);
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
        if (endTime >= latestSyncArchiveTime) map.putAll(ssdbLocal.hScan(dataTag, startTime, endTime, clazz));
        if (startTime < latestSyncArchiveTime)
            ssdbSync.hScan(dataTag, startTime, endTime + archiveConfig.getArchiveInterval()).values()
                    .parallelStream().map(Try.of(fileId -> fdfsUtil.downloadMap(fileId, clazz))).forEach(map::putAll);
        map.keySet().parallelStream().filter(key -> (Long) key < startTime || (Long) key > endTime).forEach(map::remove);
        return map;
    }

    @Override
    public <T> T getLatest(DataTag dataTag, Class<T> clazz) throws Exception {
        Long latestSyncArchiveTime = latestSyncArchiveTime(dataTag);
        long latestLocalTime = ssdbLocal.hGet(dataTag, LATEST_TIME).asLong();
        if (latestLocalTime > latestSyncArchiveTime) return ssdbLocal.hGet(dataTag, LATEST_VAL, clazz);
        else return get(dataTag, latestSyncArchiveTime, clazz);
    }

    @Override
    public void del(DataTag dataTag) {
        ssdbLocal.hClear(dataTag);
        ssdbSync.hClear(dataTag);
    }

    @Override
    public void del(DataTag dataTag, Long startTime, Long endTime) {
        ssdbLocal.hDel(dataTag, startTime, endTime);
        ssdbSync.hDel(dataTag, startTime, endTime);
    }

    //hashMap中数据归档,hashMap中key为时间戳，val为JSON对象
    @Override
    public void archiveJob() {
        archiveConfig.getArchiveTags().parallelStream().forEach(tagString -> ssdbLocal.hScanName(tagString).parallelStream().map(DataTag::new).forEach(tag -> {
            long archiveTime = System.currentTimeMillis();
            Map<String, String> valueMap = ssdbLocal.hScan(tag, latestLocalArchiveTime(tag), archiveTime);
            if (valueMap.size() != 0) {
                try {
                    String fileId = fdfsUtil.upload(JSONUtil.toMap(valueMap));
                    Map syncMap = new HashMap();
                    ssdbLocal.hSet(tag, LATEST_ARCHIVE, archiveTime);
                    syncMap.put(LATEST_ARCHIVE, archiveTime);
                    syncMap.put(archiveTime, fileId);
                    ssdbSync.hMultiSet(tag, syncMap);
                    logger.info(tag + ":" + fileId + " has archived on " + new Date(archiveTime));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else logger.info("Nothing needs to archive.");
        }));
    }

    @Override
    public void clearJob() {
        archiveConfig.getArchiveTags().parallelStream().forEach(tagString -> ssdbLocal.hScanName(tagString).parallelStream().map(DataTag::new)
                .filter(tag -> ssdbLocal.hSize(tag) > archiveConfig.getLeastClearNum()).forEach(tag -> ssdbLocal.hDel(tag, 0, latestLocalArchiveTime(tag))));
    }
}
