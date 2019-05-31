package com.cielo.tests;

import com.cielo.dbs.IoTDBMSClient;
import com.cielo.dbs.SSDBClient;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class RangeTest {
    public static final int SAMPLE_NUMBER = 10000;
    public static final int SAMPLE_LENGTH = 256;
    public List<byte[]> samples;
    private IoTDBMSClient ioTDBMSClient;
    private SSDBClient ssdbClient;

    public RangeTest() {
        ioTDBMSClient = new IoTDBMSClient(1000);
        ssdbClient = new SSDBClient(SAMPLE_NUMBER * 2);
        samples = Utils.generateBytesList(SAMPLE_LENGTH, SAMPLE_NUMBER);
    }

    public void endTest() {
        ssdbClient.flushDB();
    }

    public void ssdbUpload() {
        IntStream.range(0, SAMPLE_NUMBER).parallel().forEach(i -> ssdbClient.set(i, samples.get(i)));
    }

    public void ioTUpload() {
        IntStream.range(0, SAMPLE_NUMBER).parallel().forEach(i -> ioTDBMSClient.lazyUpload(i, samples.get(i)));
        System.out.println("Begin to archive");
        ioTDBMSClient.archive();
        System.out.println("Finish.");
    }

    public void iotTrain() {
        IntStream.range(0, SAMPLE_NUMBER).parallel().forEach(i -> ioTDBMSClient.lazyUpload(i, samples.get(i)));
        System.out.println("Begin to generate train file");
        ioTDBMSClient.generateTrainFile();
        System.out.println("Finish.");
    }

    public Map<String, byte[]> ssdbScan() {
        return ssdbClient.scanBytes(0, SAMPLE_NUMBER);
    }

    public Map<Integer, byte[]> ioTScan() {
        return ioTDBMSClient.lazyDownload(0, SAMPLE_NUMBER);
    }

    public <K, V> long timeCost(Utils.MapMethod<K, V> listMethod) {
        long startTime = System.currentTimeMillis();
        Map<K, V> map = listMethod.apply();
        System.out.println(map.size());
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public static void preTestSSDB() {
        RangeTest rangeTest = new RangeTest();
        System.out.println("Begin to upload.");
        rangeTest.ssdbUpload();
        System.out.println("Finish the upload.");
    }

    public static void testSSDB() {
        RangeTest rangeTest = new RangeTest();
        System.out.println(rangeTest.timeCost(rangeTest::ssdbScan));
    }

    public static void preTestIoT() {
        RangeTest rangeTest = new RangeTest();
        rangeTest.ioTUpload();
    }

    public static void preTrainIoT() {
        RangeTest rangeTest = new RangeTest();
        rangeTest.iotTrain();
    }

    public static void testIoT() {
        RangeTest rangeTest = new RangeTest();
        System.out.println(rangeTest.timeCost(rangeTest::ioTScan));
    }

    public static void main(String[] args) {
//        preTestSSDB();
//        testSSDB();
        preTestIoT();
//       testIoT();

    }


}
