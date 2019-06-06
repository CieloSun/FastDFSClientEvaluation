package com.cielo.tests;

import com.cielo.dbs.IoTDBMSClient;
import com.cielo.dbs.SSDBClient;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.cielo.tests.Utils.timeCost;

public class RangeTest {
    public static final int SAMPLE_NUMBER = 100000;
    public static final int SAMPLE_LENGTH = 16 * 1024;
    public List<byte[]> samples;
    private IoTDBMSClient ioTDBMSClient;
    private SSDBClient ssdbClient;

    public RangeTest() throws Exception {
        ioTDBMSClient = new IoTDBMSClient(1000);
        ssdbClient = new SSDBClient(SAMPLE_NUMBER * 2);
        samples = Utils.generateBytesList(SAMPLE_LENGTH, SAMPLE_NUMBER);
    }

    public void ssdbUpload() {
        IntStream.range(0, SAMPLE_NUMBER).parallel().forEach(i -> ssdbClient.set(i, samples.get(i)));
    }

    public Map<String, byte[]> ssdbScan() {
        return ssdbClient.scanBytes(0, SAMPLE_NUMBER);
    }

    public void endTest() {
        ssdbClient.flushDB();
    }

    public void ioTUpload() {
        IntStream.range(0, SAMPLE_NUMBER).parallel().forEach(i -> ioTDBMSClient.lazyUpload(i, samples.get(i)));
        System.out.println("Begin to archive");
        ioTDBMSClient.archive();
        System.out.println("Finish.");
    }

    public void ioTTrain() {
        IntStream.range(0, SAMPLE_NUMBER).parallel().forEach(i -> ioTDBMSClient.lazyUpload(i, samples.get(i)));
        System.out.println("Begin to generate train file");
        ioTDBMSClient.generateTrainFile();
        System.out.println("Finish.");
    }

    public Map<Integer, byte[]> ioTScan() {
        return ioTDBMSClient.lazyDownload(0, SAMPLE_NUMBER);
    }

    public static void generateTrainFile() throws Exception {
        RangeTest rangeTest = new RangeTest();
        rangeTest.ioTTrain();
    }

    public static void testSSDB() throws Exception {
        RangeTest rangeTest = new RangeTest();
        System.out.println("Begin to upload.");
        rangeTest.ssdbUpload();
        System.out.println("Finish the upload.");
        System.out.println(timeCost(rangeTest::ssdbScan));
    }

    public static void testIoT() throws Exception {
        RangeTest rangeTest = new RangeTest();
        rangeTest.ioTUpload();
        System.out.println(timeCost(rangeTest::ioTScan));
    }

    public static void main(String[] args) throws Exception {

    }
}
