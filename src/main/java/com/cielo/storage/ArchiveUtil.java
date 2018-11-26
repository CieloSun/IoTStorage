package com.cielo.storage;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ArchiveUtil {
    @Autowired
    private FDFSUtil fdfsUtil;
    @Autowired
    private SSDBUtil ssdbUtil;

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

    public void archive(String pattern) throws Exception {
        long date = System.currentTimeMillis();
        ssdbUtil.setVal(latestArchiveDateKey(pattern), date);
        ssdbUtil.set(key(pattern, date), fdfsUtil.upload(ssdbUtil.popMapValues(pattern)));
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
