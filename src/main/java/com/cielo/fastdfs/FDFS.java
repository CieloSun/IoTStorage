package com.cielo.fastdfs;

import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.StorageClient;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

@Service
@Order(3)
public class FDFS implements CommandLineRunner {
    private StorageClient storageClient;

    @Override
    public void run(String... args) throws Exception {
        ClientGlobal.init("fdfs_client.conf");
        storageClient = new StorageClient();
    }


}
