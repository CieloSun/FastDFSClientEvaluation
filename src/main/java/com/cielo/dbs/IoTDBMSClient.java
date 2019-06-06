package com.cielo.dbs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.github.luben.zstd.Zstd;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class IoTDBMSClient {
    public final static String DICTIONARY = "data.dic";
    private SSDBClient ssdbClient;
    private NIOClient nioClient;
    private int archiveSize;
    private static Map<Integer, byte[]> cache;
    private static Map<Integer, Map<Integer, byte[]>> immutableCaches;
    private byte[] compressionDictionary;

    private static void byte2file(String path, byte[] data) {
        try (FileOutputStream outputStream = new FileOutputStream(new File(path))) {
            outputStream.write(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private byte[] compress(byte[] bytes) {
        return Zstd.compressUsingDict(bytes, compressionDictionary, 3);
    }

    private byte[] decompress(byte[] bytes) {
        byte[] dst = new byte[(int) Zstd.decompressedSize(bytes)];
        Zstd.decompressUsingDict(dst, 0, bytes, 0, bytes.length, compressionDictionary);
        return dst;
    }

    private Map<Integer, byte[]> json2Map(byte[] bytes) {
        return JSON.parseObject(new String(bytes), new TypeReference<>(Integer.class, byte[].class) {
        });
    }


    private byte[] map2Json(Map map) {
        return JSON.toJSONBytes(map);
    }

    public IoTDBMSClient(int scanNum) throws Exception {
        this(scanNum, 1000);
    }

    public IoTDBMSClient(int scanNum, int archiveSize) throws Exception {
        ssdbClient = new SSDBClient(scanNum);
        nioClient = new NIOClient();
        cache = new ConcurrentHashMap<>();
        immutableCaches = new HashMap<>();
        this.archiveSize = archiveSize;
        try (FileInputStream inputStream = new FileInputStream(new File(Objects.requireNonNull(this.getClass().getClassLoader().getResource(DICTIONARY)).getPath()))) {
            compressionDictionary = new byte[inputStream.available()];
            inputStream.read(compressionDictionary);
        }
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
        immutableCaches.entrySet().parallelStream().forEach(e -> byte2file("trainDir/data_" + e.getKey() + "_" + System.currentTimeMillis() + ".jsonBytes", map2Json(e.getValue())));
        immutableCaches.clear();
    }

    public void archive() {
        immutableCaches.entrySet().parallelStream().map(e -> upload(e.getKey(), map2Json(e.getValue()))).forEach(CompletableFuture::join);
        immutableCaches.clear();
    }

    public Map<Integer, byte[]> lazyDownload(Integer fromId, Integer endId) {
        Map<Integer, byte[]> res = new ConcurrentHashMap<>();
        ssdbClient.scanMapString(fromId, endId).values().parallelStream().map(nioClient::download).map(f -> f.thenApply(this::json2Map)).map(Try.of(CompletableFuture::get)).forEach(res::putAll);
        return res;
    }

    public CompletableFuture<Void> upload(Object dataId, byte[] data) {
        return nioClient.upload(compress(data)).thenAccept(fileId -> ssdbClient.set(dataId, fileId));
    }

    public CompletableFuture<byte[]> download(Object dataId) {
        return nioClient.download(ssdbClient.get(dataId).asString()).thenApply(this::decompress);
    }
}
