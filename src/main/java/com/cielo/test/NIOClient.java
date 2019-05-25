package com.cielo.test;

import com.cielo.storage.fastdfs.FastdfsClient;
import com.cielo.storage.fastdfs.FileId;

import java.util.concurrent.CompletableFuture;

public class NIOClient {
    private FastdfsClient fastdfsClient;

    public NIOClient() {
        fastdfsClient = FastdfsClient.newBuilder().connectTimeout(3000).readTimeout(500).maxThreads(32).tracker("ws.cielosun.xyz", 22122).build();
    }

    public CompletableFuture<String> upload(byte[] content) {
        return fastdfsClient.upload("fdfs", content).thenApply(FileId::toString);
    }

    public CompletableFuture<byte[]> download(String path) {
        return fastdfsClient.download(path);
    }
}
