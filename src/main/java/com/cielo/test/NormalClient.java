package com.cielo.test;

import org.csource.fastdfs.ClientGlobal;
import org.csource.fastdfs.StorageClient;
import org.csource.fastdfs.TrackerClient;
import org.csource.fastdfs.TrackerServer;

public class NormalClient {
    private TrackerServer trackerServer;

    public NormalClient() throws Exception {
        ClientGlobal.initByTrackers("ws.cielosun.xyz:22122");
        trackerServer = new TrackerClient().getConnection();
    }

    public String upload(byte[] content) throws Exception {
        StorageClient client = new StorageClient(trackerServer, null);
        String[] fileId = client.upload_file(content, "fdfs", null);
        return fileId[0] + "/" + fileId[1];
    }

    public byte[] download(String path) throws Exception {
        StorageClient client = new StorageClient(trackerServer, null);
        String[] fileId = path.split("/", 2);
        return client.download_file(fileId[0], fileId[1]);
    }
}
