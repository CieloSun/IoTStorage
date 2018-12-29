package com.cielo.storage.core;

import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.api.SSDBUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.config.ArchiveConfig;
import com.cielo.storage.model.DataTag;
import com.cielo.storage.tool.CollectionUtil;
import com.cielo.storage.tool.JSONUtil;
import com.cielo.storage.tool.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
class TimeDataUtilImpl implements TimeDataUtil {
    private static final String LATEST = "latest_file";
    private static final DataTag TAG_SETS = new DataTag("tagSets");
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSUtil fdfsUtil;
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private ArchiveConfig archiveConfig;

    //根据数据tag生成文件tag
    private DataTag fileTag(DataTag dataTag) {
        DataTag fileTag = new DataTag("file");
        fileTag.setSubTag(dataTag);
        return fileTag;
    }

    private List<Long> searchFileTime(DataTag dataTag, Long startTime, Long endTime) {
        List<Long> fileTimeList = getFileTimeList(dataTag, startTime);
        return fileTimeList.subList(0, CollectionUtil.lowerBound(fileTimeList, endTime));
    }

    //获取某一数据tag距离给定时间戳最近的归档时间
    private Long searchFileTime(DataTag dataTag, Long timestamp) {
        List<Long> fileTimeList = getFileTimeList(dataTag, timestamp);
        return fileTimeList.get(CollectionUtil.lowerBound(fileTimeList, timestamp));
    }

    //获取某一数据tag至今的归档时间列表
    private List<Long> getFileTimeList(DataTag dataTag, Long startTime) {
        return getFileTimeList(dataTag, startTime, System.currentTimeMillis());
    }

    //获取某一数据tag某一段时间的文件列表
    private List<Long> getFileTimeList(DataTag dataTag, Long startTime, Long endTime) {
        return ssdbUtil.hScanKeys(fileTag(dataTag), startTime, endTime).parallelStream().map(timeStr -> Long.parseLong(timeStr)).collect(Collectors.toList());
    }

    //获取归档文件fileId
    private String getFileId(DataTag dataTag, Long timestamp) {
        return ssdbUtil.hGet(fileTag(dataTag), timestamp).asString();
    }

    @Override
    public void set(DataTag dataTag, Object val) {
        set(dataTag, System.currentTimeMillis(), val);
    }

    //可以手动设置时间的set方法
    @Override
    public void set(DataTag dataTag, Long timestamp, Object val) {
        ssdbUtil.hSet(dataTag, timestamp, val);
    }

    //获取可能归档的hashMap中具体某条数据
    @Override
    public <T> T get(DataTag dataTag, Long timestamp, Class<T> clazz) throws Exception {
        T t = ssdbUtil.hGet(dataTag, timestamp, clazz);
        if (t != null || !archiveConfig.isArchive()) return t;
        return fdfsUtil.downloadMap(getFileId(dataTag, searchFileTime(dataTag, timestamp)), clazz).get(timestamp);
    }

    //获取可能归档的hashMap中某段数据,由于Lambda要求所在方法可重入，因而拆分
    @Override
    public <T> Map<Object, T> get(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz) {
        if (startTime == null) startTime = 0l;
        if (endTime == null) endTime = System.currentTimeMillis();
        return getMap(dataTag, startTime, endTime, clazz);
    }

    private <T> Map<Object, T> getMap(DataTag dataTag, Long startTime, Long endTime, Class<T> clazz) {
        if (!archiveConfig.isArchive()) return ssdbUtil.hScan(dataTag, startTime, endTime, clazz);
        Map<Object, T> map = new HashMap<>();
        Long latestFileTime = getLatestFileTime(dataTag);
        if (endTime >= latestFileTime) map.putAll(ssdbUtil.hScan(dataTag, startTime, endTime, clazz));
        if (startTime < latestFileTime)
            searchFileTime(dataTag, startTime, endTime).parallelStream().map(Try.of(timestamp -> fdfsUtil.downloadMap(getFileId(dataTag, timestamp), clazz))).forEach(map::putAll);
        map.keySet().parallelStream().filter(key -> (Long) key < startTime || (Long) key > endTime).forEach(map::remove);
        return map;
    }

    //获取hashMap中最新归档时间
    @Override
    public Long getLatestFileTime(DataTag dataTag) {
        return ssdbUtil.hGet(dataTag, LATEST).asLong();
    }

    @Override
    public void clear(DataTag dataTag) {
        ssdbUtil.hClear(dataTag);
        ssdbUtil.hClear(fileTag(dataTag));
    }

    @Override
    public void del(DataTag dataTag, Long startTime, Long endTime) {
        ssdbUtil.hDel(dataTag, startTime, endTime);
        ssdbUtil.hDel(fileTag(dataTag), startTime, endTime);
    }

    //hashMap中数据归档,hashMap中key为时间戳，val为JSON对象
    @Override
    public void archive(DataTag dataTag) {
        if (!archiveConfig.isArchive()) return;
        long timestamp = System.currentTimeMillis();
        Map<String, String> map = ssdbUtil.hPopAll(dataTag).mapString();
        if (map.size() <= archiveConfig.getLeastNumber()) return;
        ssdbUtil.hSetVal(dataTag, LATEST, timestamp);
        try {
            String fileId = fdfsUtil.upload(JSONUtil.toMap(map));
            set(dataTag, timestamp, fileId);
            logger.info(dataTag + ":" + fileId + " has archived on " + new Date(timestamp));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
