package com.cielo.tests;

import com.cielo.dbs.IoTDBMSClient;
import com.cielo.dbs.NIOClient;
import com.cielo.dbs.SSDBClient;

import java.util.List;

public class RangeTest {
    public static final int SAMPLE_NUMBER = 10000;
    public static final int SAMPLE_LENGTH = 256;
    public List<byte[]> samples;
    private IoTDBMSClient ioTDBMSClient;
    private NIOClient nioClient;
    private SSDBClient ssdbClient;

    public <T> long timeCost(Utils.ClientMethod<T> clientMethod) {
        long startTime = System.currentTimeMillis();
        clientMethod.apply();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public RangeTest() {
        ioTDBMSClient = new IoTDBMSClient(1000);
        nioClient = new NIOClient();
        ssdbClient = new SSDBClient(SAMPLE_NUMBER * 2);
        samples = Utils.generateBytesList(SAMPLE_LENGTH, SAMPLE_NUMBER);
    }


}
