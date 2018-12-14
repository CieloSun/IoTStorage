package com.cielo.storage.core;

import com.cielo.storage.api.FDFSUtil;
import com.cielo.storage.api.SSDBUtil;
import com.cielo.storage.api.TimeDataUtil;
import com.cielo.storage.config.ArchiveConfig;
import com.cielo.storage.tool.CollectionUtil;
import com.cielo.storage.tool.JSONUtil;
import com.cielo.storage.tool.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
class TimeDataUtilImpl implements TimeDataUtil {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSUtil fdfsUtil;
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private ArchiveConfig archiveConfig;

    //根据不包含时间的id生成key
    private String fileKey(String prefix, Long timestamp) {
        return "file_" + prefix + "_" + timestamp;
    }

    //从一个文件key获取归档时间
    private Long getFileTime(String fileKey) {
        String[] strings = fileKey.split("_");
        return Long.parseLong(strings[strings.length - 1]);
    }

    //获取某一prefix某一时间点后的文件归档时间列表
    private List<Long> getFileTimeList(String prefix, Long timestamp) {
        return ssdbUtil.scanKeys(fileKey(prefix, timestamp), fileKey(prefix, System.currentTimeMillis())).parallelStream().map(fileKey -> getFileTime(fileKey)).sorted().collect(Collectors.toList());
    }

    @Override
    public void set(String hName, Object val) {
        ssdbUtil.hSet(hName, System.currentTimeMillis(), val);
    }

    //获取可能归档的hashMap中具体某条数据
    @Override
    public <T> T get(String hName, Long timestamp, Class<T> clazz) throws Exception {
        T t = ssdbUtil.hGet(hName, timestamp, clazz);
        if (t != null || !archiveConfig.isArchive()) return t;

        List<Long> fileTimeList = getFileTimeList(hName, timestamp);
        return fdfsUtil.downloadMap(fileKey(hName, fileTimeList.get(CollectionUtil.lowerBound(fileTimeList, timestamp))), clazz).get(timestamp);
    }

    //获取可能归档的hashMap中某段数据,由于Lambda要求所在方法可重入，因而拆分
    @Override
    public <T> Map<Object, T> get(String hName, Long startTime, Long endTime, Class<T> clazz) {
        if (startTime == null) startTime = 0l;
        if (endTime == null) endTime = System.currentTimeMillis();
        return getMap(hName, startTime, endTime, clazz);
    }

    private <T> Map<Object, T> getMap(String hName, Long startTime, Long endTime, Class<T> clazz) {
        if (!archiveConfig.isArchive()) return ssdbUtil.hScan(hName, startTime, endTime, clazz);
        Map<Object, T> map = new HashMap<>();
        Long latestFileTime = getLatestFileTime(hName);
        if (endTime >= latestFileTime) map.putAll(ssdbUtil.hScan(hName, startTime, endTime, clazz));
        if (startTime < latestFileTime) {
            List<Long> fileTimeList = getFileTimeList(hName, startTime);
            fileTimeList.subList(0, CollectionUtil.lowerBound(fileTimeList, endTime)).parallelStream().map(Try.of(timestamp -> fdfsUtil.downloadMap(fileKey(hName, timestamp), clazz))).forEach(map::putAll);
        }
        map.keySet().parallelStream().filter(key -> (Long) key < startTime || (Long) key > endTime).forEach(map::remove);
        return map;
    }

    //hashMap中数据归档,hashMap中key为时间戳，val为JSON对象
    @Override
    public void archive(String hName) {
        if (!archiveConfig.isArchive()) return;
        logger.info("Archive program in hash map begins to run.");
        long timestamp = System.currentTimeMillis();
        Map<String, String> map = ssdbUtil.hPopAll(hName).mapString();
        if (map.size() <= archiveConfig.getLeastNumber()) return;
        ssdbUtil.hSetVal(hName, LATEST, timestamp);
        String key = fileKey(hName, timestamp);
        try {
            String fileId = fdfsUtil.upload(JSONUtil.toMap(map));
            ssdbUtil.set(key, fileId);
            logger.info(key + ":" + fileId + " has archived.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取hashMap中最新归档时间
    @Override
    public Long getLatestFileTime(String hName) {
        return ssdbUtil.hGet(hName, LATEST).asLong();
    }
}
