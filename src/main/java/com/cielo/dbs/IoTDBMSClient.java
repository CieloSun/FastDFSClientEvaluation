package com.cielo.dbs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.github.luben.zstd.Zstd;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    private Map<Integer, byte[]> compressedJson2Map(byte[] bytes) {
        int length = bytes.length;
        byte[] dst = new byte[(int) Zstd.decompressedSize(bytes)];
        Zstd.decompressUsingDict(dst, 0, bytes, 0, length, compressionDictionary);
        return json2Map(dst);
    }

    private static Map<Integer, byte[]> json2Map(byte[] bytes) {
        return JSON.parseObject(new String(bytes), new TypeReference<>(Integer.class, byte[].class) {
        });
    }

    private byte[] map2CompressedJson(Map map) {
        byte[] bytes = map2Json(map);
        return Zstd.compressUsingDict(bytes, compressionDictionary, 3);
    }

    private static byte[] map2Json(Map map) {
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
        immutableCaches.entrySet().parallelStream().forEach(e -> upload(e.getKey(), map2CompressedJson(e.getValue())).join());
        immutableCaches.clear();
    }

    public Map<Integer, byte[]> lazyDownload(Integer fromId, Integer endId) {
        Map<Integer, byte[]> res = new ConcurrentHashMap<>();
        ssdbClient.scanMapString(fromId, endId).values().parallelStream().map(nioClient::download).map(f -> f.thenApply(this::compressedJson2Map)).map(Try.of(CompletableFuture::get)).forEach(res::putAll);
        return res;
    }

    public CompletableFuture<Void> upload(Object dataId, byte[] data) {
        return nioClient.upload(data).thenAccept(fileId -> ssdbClient.set(dataId, fileId));
    }

    public CompletableFuture<byte[]> download(Object dataId) {
        return nioClient.download(ssdbClient.get(dataId).asString());
    }

    public Map<Integer, byte[]> testCompression(){
        Map<Integer, byte[]> testMap = immutableCaches.entrySet().parallelStream().collect(Collectors.toMap(Map.Entry::getKey, e -> map2CompressedJson(e.getValue()), (a, b) -> b, ConcurrentHashMap::new));
        immutableCaches.clear();
        Map<Integer, byte[]> res = new ConcurrentHashMap<>();
        testMap.values().stream().map(this::compressedJson2Map).forEach(res::putAll);
        return res;
    }
}
