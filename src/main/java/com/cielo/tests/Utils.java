package com.cielo.tests;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utils {
    @FunctionalInterface
    public interface ClientMethod<T> {
        List<T> apply();
    }
    public static List<byte[]> generateBytesList(int length, int size) {
        return IntStream.range(0, size).mapToObj(i -> new byte[length]).map(bytes -> {
            Arrays.fill(bytes, (byte) 0x32);
            return bytes;
        }).collect(Collectors.toList());
    }
}
