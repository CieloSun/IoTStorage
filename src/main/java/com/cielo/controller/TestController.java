package com.cielo.controller;

import com.cielo.fastdfs.FDFS;
import org.csource.fastdfs.FileInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@CrossOrigin
public class TestController {
    @Autowired
    private FDFS fdfs;

    @GetMapping("upload")
    public String upload(String upString) throws Exception {
        return fdfs.upload(upString);
    }

    @GetMapping("download")
    public String download(String path) throws Exception {
        return fdfs.download(path);
    }

    @GetMapping("info")
    public FileInfo getFileInfo(String path) throws Exception {
        return fdfs.info(path);
    }
}
