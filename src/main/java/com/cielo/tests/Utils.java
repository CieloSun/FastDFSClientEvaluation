package com.cielo.tests;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utils {
    @FunctionalInterface
    public interface ListMethod<T> {
        List<T> apply();
    }

    @FunctionalInterface
    public interface MapMethod<K, V> {
        Map<K, V> apply();
    }

    @FunctionalInterface
    public interface voidMethod {
        void apply();
    }

    public static List<byte[]> generateBytesList(int length, int size) {
        List<byte[]> collect = IntStream.range(0, size).mapToObj(i -> {
            byte[] bytes = new byte[length];
            new Random().nextBytes(bytes);
            return bytes;
        }).collect(Collectors.toList());
        System.out.println("Finish to generate random data, length " + length + ", size " + size + ".");
        return collect;
    }

    public static long timeCost(voidMethod voidMethod) {
        long startTime = System.currentTimeMillis();
        voidMethod.apply();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }

    public static <K, V> long timeCost(MapMethod<K, V> mapMethod) {
        long startTime = System.currentTimeMillis();
        Map<K, V> map = mapMethod.apply();
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
}
