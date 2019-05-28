package com.cielo.test;

import com.cielo.storage.fastdfs.FileId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Test {
    public static final int SAMPLE_NUMBER = 500;
    public static final int SAMPLE_LENGTH = 256;
    public static final int JDBC_CONNECTION_NUM = 50;

    public NIOClient nioClient;
    public NormalClient normalClient;
    public MySQLClient mySQLClient;

    public List<byte[]> samples;
    public List<String> paths;
    public List<byte[]> results;
    public long timeCost;

    @FunctionalInterface
    public interface ClientMethod<T> {
        List<T> apply();
    }

    public Test() throws Exception {
        samples = new ArrayList<>();
        nioClient = new NIOClient();
        normalClient = new NormalClient();
        mySQLClient = new MySQLClient(JDBC_CONNECTION_NUM);
        IntStream.range(0, SAMPLE_NUMBER).mapToObj(i -> new byte[SAMPLE_LENGTH]).forEach(bytes -> {
            Arrays.fill(bytes, (byte) 0x32);
            samples.add(bytes);
        });
    }

    public List<FileId> nioUpload() {
        return samples.parallelStream().map(nioClient::upload).map(Try.of(CompletableFuture::get)).collect(Collectors.toList());
    }

    public List<byte[]> nioDownload() {
        return paths.parallelStream().map(nioClient::download).map(Try.of(CompletableFuture::get)).collect(Collectors.toList());
    }

    public List<String> normalUpload() {
        return samples.parallelStream().map(Try.of(normalClient::upload)).collect(Collectors.toList());
    }

    public List<byte[]> normalDownload() {
        return paths.parallelStream().map(Try.of(normalClient::download)).collect(Collectors.toList());
    }

    public List<String> mySQLUpload() {
        List<String> res = new ArrayList<>();
        samples.parallelStream().forEach(s -> {
            String dataId = UUID.randomUUID().toString().replace("-", "");
            mySQLClient.upload(dataId, s);
            res.add(dataId);
        });
        return res;
    }

    public List<byte[]> mySQLDownload() {
        return paths.parallelStream().map(Try.of(mySQLClient::download)).collect(Collectors.toList());
    }

    public <T> List<T> timeCost(ClientMethod<T> clientMethod) {
        long startTime = System.currentTimeMillis();
        List<T> list = clientMethod.apply();
        long endTime = System.currentTimeMillis();
        timeCost = endTime - startTime;
        return list;
    }

    public static long[] testNio() throws Exception {
        Test nioTest = new Test();
        nioTest.paths = nioTest.timeCost(nioTest::nioUpload).stream().map(FileId::toString).collect(Collectors.toList());
        long uploadTimeCost = nioTest.timeCost;
        nioTest.results = nioTest.timeCost(nioTest::nioDownload);
        long downloadTimeCost = nioTest.timeCost;
        return new long[]{uploadTimeCost, downloadTimeCost};
    }

    public static void testNormal() throws Exception {
        System.out.println("normal");
        Test normalTest = new Test();
        normalTest.paths = normalTest.timeCost(normalTest::normalUpload);
        System.out.println("upload time cost " + normalTest.timeCost + " ms");
        normalTest.results = normalTest.timeCost(normalTest::normalDownload);
        System.out.println("download time cost " + normalTest.timeCost + " ms");
    }

    public static long[] testMySQL() throws Exception {
        Test mySQLTest = new Test();
        mySQLTest.paths = mySQLTest.timeCost(mySQLTest::mySQLUpload);
        long uploadTimeCost = mySQLTest.timeCost;
        mySQLTest.results = mySQLTest.timeCost(mySQLTest::mySQLDownload);
        long downloadTimeCost = mySQLTest.timeCost;
        return new long[]{uploadTimeCost, downloadTimeCost};
    }

    public static void testAverage(String... args) throws Exception {
        System.out.println(args[0]);
        int cnt = 4;
        long uploadTimeCost = 0;
        long downloadTimeCost = 0;
        while (cnt-- > 0) {
            long[] longs;
            if (args[0].equals("mySQL")) longs = testMySQL();
            else longs = testNio();
            if (cnt < 3) {
                uploadTimeCost += longs[0];
                downloadTimeCost += longs[1];
            }
        }
        uploadTimeCost /= 3;
        downloadTimeCost /= 3;
        System.out.println("upload time cost " + uploadTimeCost + " ms");
        System.out.println("download time cost " + downloadTimeCost + " ms");
    }

    public static void main(String[] args) throws Exception {
        //testNormal();
        testAverage("mySQL");
    }
}
