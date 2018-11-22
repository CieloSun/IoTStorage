package com.cielo.service;

import com.cielo.fastdfs.FDFS;
import com.cielo.ssdb.SSDBUtil;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Configuration
@ConfigurationProperties("archive")
@Data
public class ArchiveService {
    private List<String> keys;
    @Autowired
    private FDFS fdfs;
    @Autowired
    private SSDBUtil ssdbUtil;

    public String key(String pattern, Long date) {
        return "file_" + pattern + "_" + date;
    }

    public String archeiveKey(String pattern) {
        return "latest_file_" + pattern;
    }

    @Async
    public void archive() throws Exception {
        for (String key : keys) {
            long date = System.currentTimeMillis();
            ssdbUtil.set(key(key, date), fdfs.upload(ssdbUtil.getListString(key)));
            ssdbUtil.setVal(archeiveKey(key), date);
        }
    }
}
