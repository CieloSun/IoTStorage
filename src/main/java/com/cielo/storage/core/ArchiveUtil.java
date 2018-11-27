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

    private Long getDateFromKey(String key) {
        return Long.parseLong(key.split("_")[2]);
    }

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


    public <T> List<T> getObjects(String pattern, Long date, Class<T> clazz) throws Exception {
        List<Long> dates = ssdbUtil.getMapKeys(filePattern(pattern)).parallelStream().map(key -> getDateFromKey(key)).sorted().collect(Collectors.toList());
        return fdfsUtil.download(key(pattern, dates.get(CollectionUtil.lowerBound(dates, date))), clazz);
    }

    public <T> List<T> getObjects(String pattern, Long startDate, Long endDate, Class<T> clazz) {
        List<Long> dates = ssdbUtil.getMapKeys(filePattern(pattern)).parallelStream().map(key -> getDateFromKey(key)).filter(fileDate -> fileDate >= startDate).sorted().collect(Collectors.toList());
        return JSONUtil.mergeList(dates.subList(0, CollectionUtil.lowerBound(dates, endDate)).parallelStream().map(Try.of(date -> fdfsUtil.download(key(pattern, date)))).collect(Collectors.toList()), clazz);
    }
}
