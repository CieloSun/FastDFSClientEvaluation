package com.cielo.dbs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class IoTDBMSClient {
    private SSDBClient ssdbClient;
    private NIOClient nioClient;
    private int scanNum;
    private Map<String, byte[]> cache;
    private String latestId;

    private static Map<String, byte[]> json2Map(byte[] bytes) {
        return JSON.parseObject(new String(bytes), new TypeReference<>(String.class, byte[].class) {
        });
    }

    private boolean inRange(String keyStr, int from, int end) {
        int key = Integer.parseInt(keyStr);
        return key >= from && key <= end;
    }


    public IoTDBMSClient(int scanNum) {
        ssdbClient = new SSDBClient(scanNum);
        nioClient = new NIOClient();
        cache = new HashMap<>(2 * scanNum);
        this.scanNum = scanNum;
    }

    public void lazyUpload(String dataId, byte[] data) {
        cache.put(dataId, data);
        if (cache.size() == scanNum) {
            upload(dataId, JSON.toJSONBytes(cache));
            latestId = dataId;
            cache.clear();
        }
    }

    public Map<String, byte[]> lazyDownload(String fromId, String endId) {
        int latest = Integer.parseInt(latestId);
        int from = Integer.parseInt(fromId);
        int end = Integer.parseInt(endId);
        Map<String, byte[]> res = new HashMap<>();
        String ssdbEndKey;
        if (end > latest) {
            cache.entrySet().stream().filter(e -> inRange(e.getKey(), from, end)).forEach(e -> res.put(e.getKey(), e.getValue()));
            ssdbEndKey = latestId;
        } else ssdbEndKey = endId;
        if (from <= latest)
            ssdbClient.scan(fromId, ssdbEndKey).values().parallelStream().map(this::download).map(f -> f.thenApply(IoTDBMSClient::json2Map)).map(Try.of(CompletableFuture::get)).forEach(res::putAll);
        return res;
    }

    public void upload(String dataId, byte[] data) {
        nioClient.upload(data).thenAccept(fileId -> ssdbClient.set(dataId, fileId)).join();
    }

    public CompletableFuture<byte[]> download(String dataId) {
        return nioClient.download(ssdbClient.get(dataId).asString());
    }
}
