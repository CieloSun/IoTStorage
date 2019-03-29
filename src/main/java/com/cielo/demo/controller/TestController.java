package com.cielo.demo.controller;

import com.cielo.storage.api.PersistUtil;
import com.cielo.storage.api.TimeDataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@CrossOrigin
@RequestMapping("test")
public class TestController {
    @Autowired
    private PersistUtil persistUtil;
    @Autowired
    private TimeDataUtil timeDataUtil;

    @GetMapping("upload")
    public String upload(String content) throws Exception {
        return persistUtil.upload(content);
    }

    @GetMapping("upMeta")
    public String uploadWithMeta(String content) throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put("tag", "test");
        map.put("timestamp", Long.toString(System.currentTimeMillis()));
        return persistUtil.upload(content, map);
    }

    @GetMapping("download")
    public String download(String path) throws Exception {
        return persistUtil.downloadString(path);
    }

    @GetMapping("archive")
    public void archive() {
        timeDataUtil.archiveJob();
    }
}
