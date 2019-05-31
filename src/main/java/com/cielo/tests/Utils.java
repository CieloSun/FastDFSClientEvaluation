package com.cielo.tests;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    public static List<byte[]> generateBytesList(int length, int size) {
        System.out.println("Start to generate random data, length " + length + ", size " + size + ".");
        List<byte[]> collect = IntStream.range(0, size).mapToObj(i -> {
            byte[] bytes = new byte[length];
            Arrays.fill(bytes, (byte) i);
            return bytes;
        }).collect(Collectors.toList());
        System.out.println("Finish the generation.");
        return collect;
    }
}
