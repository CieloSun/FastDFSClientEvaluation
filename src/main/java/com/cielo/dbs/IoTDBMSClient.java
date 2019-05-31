package com.cielo.dbs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class IoTDBMSClient {
    private SSDBClient ssdbClient;
    private NIOClient nioClient;
    private int archiveSize;
    private static Map<Integer, byte[]> cache;
    private static Map<Integer, Map<Integer, byte[]>> immutableCaches;

    public static void byte2file(String path, byte[] data) {
        try {
            FileOutputStream outputStream = new FileOutputStream(new File(path));
            outputStream.write(data);
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Map<Integer, byte[]> json2Map(byte[] bytes) {
        return JSON.parseObject(new String(bytes), new TypeReference<>(Integer.class, byte[].class) {
        });
    }

    private static byte[] map2Json(Map map) {
        byte[] bytes = JSON.toJSONBytes(map);
        System.out.println(bytes.length);
        return bytes;
    }

    private boolean inRange(Object key, Object from, Object end) {
        Integer keyInt = (Integer) key;
        return keyInt >= (Integer) from && keyInt <= (Integer) end;
    }

    public IoTDBMSClient(int scanNum) {
        this(scanNum, 1000);
    }

    public IoTDBMSClient(int scanNum, int archiveSize) {
        ssdbClient = new SSDBClient(scanNum);
        nioClient = new NIOClient();
        cache = new ConcurrentHashMap<>();
        immutableCaches = new HashMap<>();
        this.archiveSize = archiveSize;
    }

    public void lazyUpload(Integer dataId, byte[] data) {
        cache.put(dataId, data);
        if (cache.size() >= archiveSize) {
            Map<Integer, byte[]> immutableCache = cache;
            immutableCaches.put(dataId, immutableCache);
            cache = new ConcurrentHashMap<>();
        }
    }

    public void generateTrainFile() {
        immutableCaches.entrySet().parallelStream().forEach(e -> byte2file("trainDir/data_" + e.getKey() + ".jsonBytes", map2Json(e.getValue())));
        immutableCaches.clear();
    }

    public void archive() {
        immutableCaches.entrySet().parallelStream().forEach(e -> upload(e.getKey(), map2Json(e.getValue())).join());
        immutableCaches.clear();
    }

    public Map<Integer, byte[]> lazyDownload(Integer fromId, Integer endId) {
        Map<Integer, byte[]> res = new ConcurrentHashMap<>();
        ssdbClient.scanMapString(fromId, endId).values().parallelStream().map(nioClient::download).map(f -> f.thenApply(IoTDBMSClient::json2Map)).map(Try.of(CompletableFuture::get)).forEach(res::putAll);
        return res;
    }

    public CompletableFuture<Void> upload(Object dataId, byte[] data) {
        return nioClient.upload(data).thenAccept(fileId -> ssdbClient.set(dataId, fileId));
    }

    public CompletableFuture<byte[]> download(Object dataId) {
        return nioClient.download(ssdbClient.get(dataId).asString());
    }
}
