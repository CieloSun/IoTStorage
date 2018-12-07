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

//用于管理小数据，采用小数据合并的方式归档SSDB中数据
@Service
public class ArchiveUtil {
    Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private FDFSUtil fdfsUtil;
    @Autowired
    private SSDBUtil ssdbUtil;
    @Autowired
    private ArchiveConfig archiveConfig;

    private String filePattern(String pattern) {
        return "file_" + pattern;
    }

    private String key(String pattern, Long date) {
        return "file_" + pattern + "_" + date;
    }

    private String latestArchiveDateKey(String pattern) {
        return "latest_file_" + pattern;
    }

    private Long getFileDateFromKey(String key) {
        String[] strings = key.split("_");
        return Long.parseLong(strings[strings.length - 1]);
    }

    //小数据采用按周期归档，每次归档每个pattern整理成一个文件的方式
    public void archive(String pattern) {
        logger.info("Archive program begins to run.");
        long date = System.currentTimeMillis();
        ssdbUtil.setVal(latestArchiveDateKey(pattern), date);
        String key = key(pattern, date);
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

    public Long getLatestArchiveDate(String pattern) {
        return ssdbUtil.get(latestArchiveDateKey(pattern)).asLong();
    }

    public <T> List<T> get(String pattern, Long date, Class<T> clazz) throws Exception {
        List<Long> dates = ssdbUtil.getMapKeys(filePattern(pattern)).parallelStream().map(key -> getFileDateFromKey(key)).sorted().collect(Collectors.toList());
        return fdfsUtil.download(key(pattern, dates.get(CollectionUtil.lowerBound(dates, date))), clazz);
    }

    public <T> List<T> get(String pattern, Long startDate, Long endDate, Class<T> clazz) {
        List<Long> dates = ssdbUtil.getMapKeys(filePattern(pattern)).parallelStream().map(key -> getFileDateFromKey(key)).filter(fileDate -> fileDate >= startDate).sorted().collect(Collectors.toList());
        return JSONUtil.mergeList(dates.subList(0, CollectionUtil.lowerBound(dates, endDate)).parallelStream().map(Try.of(date -> fdfsUtil.download(key(pattern, date)))).collect(Collectors.toList()), clazz);
    }

    //大数据单独归档，用户手动控制，较为简单

    public void archiveOne(String key) throws Exception {
        ssdbUtil.set(key, fdfsUtil.upload(ssdbUtil.get(key).asString()));
    }

    public <T> T getOne(String key, Class<T> clazz) throws Exception {
        return fdfsUtil.downloadObject(key, clazz);
    }
}
