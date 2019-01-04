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

import java.util.*;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
class TimeDataUtilImpl implements TimeDataUtil {
    private static final DataTag LATEST_FILE = new DataTag("latest_file");
    private static final String LATEST_VAL = "latest_val";
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

    //获取hashMap中最新归档时间
    private Long getLatestArchiveTime(DataTag dataTag) {
        Response response = ssdbSync.hGet(LATEST_FILE, dataTag.toString());
        if (response.notFound()) return 0L;
        return response.asLong();
    }

    //获取某一数据tag距离给定时间戳最近的归档时间
    private String searchFile(DataTag dataTag, Long timestamp) {
        Map<String, String> files = ssdbSync.hScan(dataTag, timestamp, timestamp + archiveConfig.getArchiveInterval());
        List<Long> timeList = CollectionUtil.parseLongList(files.keySet());
        return files.get(CollectionUtil.lowerBoundVal(timeList, timestamp));
    }

    //该方法仅做一个大概搜索，搜索的内容可能略多于所要的内容
    private Collection<String> searchFile(DataTag dataTag, Long startTime, Long endTime) {
        return ssdbSync.hScan(dataTag, startTime, endTime + archiveConfig.getArchiveInterval()).values();
    }

    private <T> Map<Object, T> getMap(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz) {
        Map<Object, T> map = new HashMap<>();
        Long latestFileTime = getLatestArchiveTime(dataTag);
        if (endTime >= latestFileTime) map.putAll(ssdbLocal.hScan(dataTag, startTime, endTime, clazz));
        if (startTime < latestFileTime)
            searchFile(dataTag, startTime, endTime).parallelStream().map(Try.of(fileId -> fdfsUtil.downloadMap(fileId, clazz))).forEach(map::putAll);
        map.keySet().parallelStream().filter(key -> (Long) key < startTime || (Long) key > endTime).forEach(map::remove);
        return map;
    }

    @Override
    public void set(DataTag dataTag, Object val) {
        val = JSON.toJSONString(val);
        Map map = new HashMap();
        map.put(System.currentTimeMillis(), val);
        map.put(LATEST_VAL, val);
        ssdbLocal.hMultiSet(dataTag, map);
    }

    //获取可能归档的hashMap中具体某条数据
    @Override
    public <T> T get(DataTag dataTag, Long timestamp, Class<T> clazz) throws Exception {
        T t = ssdbLocal.hGet(dataTag, timestamp, clazz);
        if (t != null) return t;
        return fdfsUtil.downloadMap(searchFile(dataTag, timestamp), clazz).get(timestamp);
    }

    //获取可能归档的hashMap中某段数据,由于Lambda要求所在方法可重入，因而拆分
    @Override
    public <T> Map<Object, T> get(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz) {
        if (startTime == null) startTime = 0L;
        if (endTime == null) endTime = System.currentTimeMillis();
        return getMap(dataTag, startTime, endTime, clazz);
    }

    @Override
    public <T> T getLatest(DataTag dataTag, Class<T> clazz) {
        return ssdbLocal.hGet(dataTag, LATEST_VAL, clazz);
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
            Map<String, String> map = ssdbLocal.hScan(tag, getLatestArchiveTime(tag), archiveTime);
            if (map.size() != 0) {
                ssdbSync.hSetVal(LATEST_FILE, tag.toString(), archiveTime);
                try {
                    String fileId = fdfsUtil.upload(JSONUtil.toMap(map));
                    ssdbSync.hSetVal(tag, archiveTime, fileId);
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
                .filter(tag -> ssdbLocal.hSize(tag) > archiveConfig.getLeastClearNum()).forEach(tag -> ssdbLocal.hDel(tag, 0, getLatestArchiveTime(tag))));
    }
}
