package com.cielo.storage.core;

import com.cielo.storage.config.ArchiveConfig;
import com.cielo.storage.tool.CollectionUtil;
import com.cielo.storage.tool.JSONUtil;
import com.cielo.storage.tool.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//用于管理较小value，采用小数据合并的方式归档SSDB中数据
@Service
public class ArchiveUtil {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSUtil fdfsUtil;
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private ArchiveConfig archiveConfig;

    //根据原id生成文件id
    private String fileKey(String key) {
        return "file_" + key;
    }

    //根据不包含时间的id生成key
    private String fileKey(String pattern, Long timestamp) {
        return "file_" + pattern + "_" + timestamp;
    }

    //一个pattern对应的最新文件的key
    private String latestFileKey(String pattern) {
        return "latest_file_" + pattern;
    }

    //从一个文件key获取归档时间
    private Long getFileTime(String fileKey) {
        String[] strings = fileKey.split("_");
        return Long.parseLong(strings[strings.length - 1]);
    }

    //小数据采用按周期归档，每次归档每个pattern整理成一个文件的方式
    public void archive(String pattern) {
        logger.info("Archive program begins to run.");
        long date = System.currentTimeMillis();
        ssdbUtil.setVal(latestFileKey(pattern), date);
        String key = fileKey(pattern, date);
        Map<String, String> map = ssdbUtil.popMap(pattern);
        if (map.size() <= archiveConfig.getLeastNumber()) return;
        try {
            String fileId = fdfsUtil.upload(JSONUtil.merge(map.values().parallelStream().collect(Collectors.toList())));
            ssdbUtil.set(key, fileId);
            logger.info(key + ":" + fileId + " has archived.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //获取最新归档时间
    public Long getLatestFileTime(String pattern) {
        return ssdbUtil.get(latestFileKey(pattern)).asLong();
    }

    //获取某时间点的归档文件中数据
    public <T> List<T> get(String pattern, Long timestamp, Class<T> clazz) throws Exception {
        List<Long> fileTimeList = ssdbUtil.getKeys(fileKey(pattern)).parallelStream().map(fileKey -> getFileTime(fileKey)).sorted().collect(Collectors.toList());
        return fdfsUtil.download(fileKey(pattern, fileTimeList.get(CollectionUtil.lowerBound(fileTimeList, timestamp))), clazz);
    }

    //获取某段时间的归档文件数据
    public <T> List<T> get(String pattern, Long startTime, Long endTime, Class<T> clazz) {
        List<Long> fileTimeList = ssdbUtil.getKeys(fileKey(pattern, startTime), fileKey(pattern, Long.MAX_VALUE)).parallelStream().map(key -> getFileTime(key)).sorted().collect(Collectors.toList());
        return JSONUtil.mergeList(fileTimeList.subList(0, CollectionUtil.lowerBound(fileTimeList, endTime)).parallelStream().map(Try.of(timestamp -> fdfsUtil.download(fileKey(pattern, timestamp)))).collect(Collectors.toList()), clazz);
    }
}
