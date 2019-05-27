package com.cielo.test;

import com.cielo.storage.fastdfs.FileId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Test {
    public static final int SAMPLE_NUMBER = 10;
    public static final int SAMPLE_LENGTH = 256;
    public static final int THREAD_NUM = 10;
    public List<byte[]> samples;
    public List<String> paths;
    public List<byte[]> results;
    public NIOClient nioClient;
    public NormalClient normalClient;
    public long timeCost;

    @FunctionalInterface
    public interface ClientMethod<T> {
        List<T> apply();
    }

    public Test() throws Exception {
        samples = new ArrayList<>();
        nioClient = new NIOClient();
        normalClient = new NormalClient();
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

    public <T> List<T> timeCost(ClientMethod<T> clientMethod) {
        long startTime = System.currentTimeMillis();
        List<T> list = clientMethod.apply();
        long endTime = System.currentTimeMillis();
        timeCost = endTime - startTime;
        return list;
    }

    public static void testNio() throws Exception {
        System.out.println("nio");
        Test nioTest = new Test();
        System.out.println("sample size " + nioTest.samples.size());
        nioTest.paths = nioTest.timeCost(nioTest::nioUpload).stream().map(FileId::toString).collect(Collectors.toList());
        System.out.println("upload time cost " + nioTest.timeCost + " ms");
        System.out.println("file id size " + nioTest.paths.size());
        nioTest.results = nioTest.timeCost(nioTest::nioDownload);
        System.out.println("download time cost " + nioTest.timeCost + " ms");
        System.out.println("result size " + nioTest.results.size());
    }

    public static void testNormal() throws Exception {
        System.out.println("normal");
        Test normalTest = new Test();
        System.out.println("sample size " + normalTest.samples.size());
        normalTest.paths = normalTest.timeCost(normalTest::normalUpload);
        System.out.println("upload time cost " + normalTest.timeCost + " ms");
        System.out.println("file id size " + normalTest.paths.size());
        normalTest.results = normalTest.timeCost(normalTest::normalDownload);
        System.out.println("download time cost " + normalTest.timeCost + " ms");
        System.out.println("result size " + normalTest.results.size());
    }

    public static void main(String[] args) throws Exception {
        testNio();
        testNormal();
    }
}
