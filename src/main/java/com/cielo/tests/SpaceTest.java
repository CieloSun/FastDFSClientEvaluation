package com.cielo.tests;

import com.cielo.dbs.IoTDBMSClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

public class SpaceTest {
    public static final int SAMPLE_NUMBER = 10000;
    public static final int SAMPLE_LENGTH = 16 * 1024;
    private IoTDBMSClient ioTDBMSClient;
    public List<byte[]> samples;

    public SpaceTest() throws Exception {
        ioTDBMSClient = new IoTDBMSClient(1000);
        samples = Utils.generateBytesList(SAMPLE_LENGTH, SAMPLE_NUMBER);
    }

    public void spaceTest() {
        for (int s = 0; s < SAMPLE_NUMBER; s += 10) {
            IntStream.range(s, s + 10).parallel().mapToObj(i -> ioTDBMSClient.upload(i, samples.get(i))).forEach(CompletableFuture::join);
            System.out.println(s + " to " + (s + 10) + " sample has uploaded.");
        }
    }

    public static void main(String[] args) throws Exception {
    }
}
